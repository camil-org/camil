package io.github.party

import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object ShoppingList:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class ListItem(owner: String, item: String)
  final case class ShoppingState(counter: Int, items: List[ListItem])
  final case class Reply(to: String, message: String)

  sealed trait ShoppingCommand
  final case class IncreaseCounter(delta: Int) extends ShoppingCommand
  final case class AddItem(owner: String, item: String) extends ShoppingCommand
  case object ClearList extends ShoppingCommand

  final case class ClientCommand(from: String, command: ShoppingCommand)

  private sealed trait ServerEvent
  private final case class ReplyIssued(reply: Reply) extends ServerEvent
  private final case class StatePublished(state: ShoppingState) extends ServerEvent

  private val clientIds = List("Mom", "Dad", "John")
  private val initialCounter = 10
  private val initialStateDelayMs = 250L
  private val betweenCommandsDelayMs = 120L
  private val startupDelayMs: Map[String, Long] = Map(
    "Mom" -> 600L,
    "Dad" -> 1000L,
    "John" -> 1400L,
  )

  private val scripts: Map[String, List[ShoppingCommand]] = Map(
    "Mom" -> List(
      AddItem("Mom", "milk"),
      IncreaseCounter(1),
    ),
    "Dad" -> List(
      AddItem("Dad", "bread"),
      IncreaseCounter(2),
    ),
    "John" -> List(
      AddItem("John", "apples"),
      ClearList,
      AddItem("John", "tea"),
    ),
  )

  private def renderCommand(command: ShoppingCommand): String =
    command match
      case IncreaseCounter(delta) => s"IncreaseCounter($delta)"
      case AddItem(owner, item) => s"""AddItem("$owner", "$item")"""
      case ClearList => "ClearList"

  private def renderItems(items: List[ListItem]): String =
    if items.isEmpty then "<empty>"
    else items.map(item => s"${item.item} (${item.owner})").mkString(" | ")

  private def renderState(state: ShoppingState): String =
    s"counter=${state.counter}, items=${renderItems(state.items)}"

  def shoppingListProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val commandsOnClient = on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val script = scripts.getOrElse(clientId, List.empty)
      val startupDelay = startupDelayMs.getOrElse(clientId, 500L)
      signalBuilder[ClientCommand]: emitter =>
        Thread.sleep(startupDelay)
        script.foreach: command =>
          println(s"[Client $clientId] RPC -> ${renderCommand(command)}")
          emitter.emit(ClientCommand(clientId, command))
          Thread.sleep(betweenCommandsDelayMs)

    val serverEventsOnServer = on[Server]:
      val commandStreams = asLocalAll[Server, Client](commandsOnClient).toMap
      println(s"[Server] Tracking ${commandStreams.size} shopping client stream(s).")

      val mergedCommands = Signal.merge(commandStreams.values.toSeq)
      val (eventSignal, eventEmitter) = Signal.make[ServerEvent]
      val allStreamsClosed = CountDownLatch(commandStreams.size)
      val lock = new AnyRef
      var state = ShoppingState(initialCounter, List.empty)

      Future:
        Thread.sleep(initialStateDelayMs)
        val snapshot = lock.synchronized(state)
        println(s"[Server] initial state -> ${renderState(snapshot)}")
        eventEmitter.emit(StatePublished(snapshot))

      commandStreams.toSeq
        .sortBy(_._1.toString)
        .foreach:
          case (clientId, stream) =>
            stream.onClose: () =>
              allStreamsClosed.countDown()
              println(s"[Server] Command stream closed for $clientId (${allStreamsClosed.getCount} remaining).")

      mergedCommands.subscribe: clientCommand =>
        val (reply, snapshot) = lock.synchronized:
          clientCommand.command match
            case IncreaseCounter(delta) =>
              state = state.copy(counter = state.counter + delta)
              Reply(clientCommand.from, s"Counter increased by $delta. Current value: ${state.counter}") -> state
            case AddItem(owner, item) =>
              state = state.copy(items = ListItem(owner, item) :: state.items)
              Reply(clientCommand.from, s"Added $item for $owner.") -> state
            case ClearList =>
              state = state.copy(items = List.empty)
              Reply(clientCommand.from, "List cleared.") -> state
        println(s"[Server] ${clientCommand.from} -> ${renderCommand(clientCommand.command)}")
        println(s"[Server] state -> ${renderState(snapshot)}")
        eventEmitter.emit(ReplyIssued(reply))
        eventEmitter.emit(StatePublished(snapshot))

      Future:
        allStreamsClosed.await()
        val finalState = lock.synchronized(state)
        println(s"[Server] Final shopping state -> ${renderState(finalState)}")
        eventEmitter.close()

      eventSignal

    val repliesOnServer = on[Server]:
      val events = take(serverEventsOnServer)
      val (replySignal, replyEmitter) = Signal.make[Reply]
      events.subscribe:
        case ReplyIssued(reply) => replyEmitter.emit(reply)
        case _ => ()
      events.onClose: () =>
        replyEmitter.close()
      replySignal

    val statesOnServer = on[Server]:
      val events = take(serverEventsOnServer)
      val (stateSignal, stateEmitter) = Signal.make[ShoppingState]
      events.subscribe:
        case StatePublished(state) => stateEmitter.emit(state)
        case _ => ()
      events.onClose: () =>
        stateEmitter.close()
      stateSignal

    on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val incomingReplies = asLocal[Client, Server](repliesOnServer)
      val incomingStates = asLocal[Client, Server](statesOnServer)
      val done = CountDownLatch(2)

      incomingReplies.subscribe: reply =>
        if reply.to == clientId then println(s"[Client $clientId] ack -> ${reply.message}")

      incomingReplies.onClose: () =>
        println(s"[Client $clientId] reply stream closed.")
        done.countDown()

      incomingStates.subscribe: state =>
        println(s"[Client $clientId] state -> ${renderState(state)}")

      incomingStates.onClose: () =>
        println(s"[Client $clientId] state stream closed.")
        done.countDown()

      done.await()
  end shoppingListProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running ShoppingList scripted port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val momNetwork = InMemoryNetwork[Client]("Mom", broker)
    val dadNetwork = InMemoryNetwork[Client]("Dad", broker)
    val johnNetwork = InMemoryNetwork[Client]("John", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(shoppingListProtocol) }
    val momFuture = Future { handleProgramForPeer[Client](momNetwork)(shoppingListProtocol) }
    val dadFuture = Future { handleProgramForPeer[Client](dadNetwork)(shoppingListProtocol) }
    val johnFuture = Future { handleProgramForPeer[Client](johnNetwork)(shoppingListProtocol) }

    val all = Future.sequence(Seq(serverFuture, momFuture, dadFuture, johnFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("ShoppingList done.")
end ShoppingList
