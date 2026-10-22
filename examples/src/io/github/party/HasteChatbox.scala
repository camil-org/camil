package io.github.party

import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.Network.*
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object HasteChatbox:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  sealed trait ClientCommand
  final case class Hello(sessionId: String) extends ClientCommand
  final case class Send(sessionId: String, text: String) extends ClientCommand
  final case class Bye(sessionId: String) extends ClientCommand

  final case class Delivery(to: String, from: String, text: String)

  private val scripts: Map[String, List[String]] = Map(
    "alice" -> List("Hi, I am Alice.", "Anyone checked section 2.3?"),
    "bob" -> List("Bob here.", "I like the typed remote bridge."),
    "carol" -> List("Carol joined.", "Looks good from my side."),
  )

  private val startupDelayMs = 700L
  private val betweenMessagesDelayMs = 180L
  private val beforeByeDelayMs = 180L
  private val awaitPollTimeoutMs = 150L

  def chatboxProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val commandsOnClient = on[Client]:
      val sessionId = peerAddress.asInstanceOf[String]
      val messages = scripts.getOrElse(sessionId, List(s"Hello from $sessionId"))
      signalBuilder[ClientCommand]: emitter =>
        Thread.sleep(startupDelayMs)
        println(s"[Client $sessionId] hello")
        emitter.emit(Hello(sessionId))
        messages.foreach: message =>
          Thread.sleep(betweenMessagesDelayMs)
          println(s"[Client $sessionId] send '$message'")
          emitter.emit(Send(sessionId, message))
        Thread.sleep(beforeByeDelayMs)
        println(s"[Client $sessionId] bye")
        emitter.emit(Bye(sessionId))

    val deliveriesOnServer = on[Server]:
      val commandStreams = asLocalAll[Server, Client](commandsOnClient).toMap
      println(s"[Server] Tracking ${commandStreams.size} client stream(s).")

      val mergedCommands = Signal.merge(commandStreams.values.toSeq)
      val (deliverySignal, deliveryEmitter) = Signal.make[Delivery]
      val activeRecipients = scala.collection.mutable.Set.empty[String]
      val lock = new AnyRef
      val allStreamsClosed = new CountDownLatch(commandStreams.size)

      commandStreams.toSeq
        .sortBy(_._1.toString)
        .foreach:
          case (clientId, stream) =>
            stream.onClose: () =>
              allStreamsClosed.countDown()
              println(s"[Server] Stream closed for $clientId (${allStreamsClosed.getCount} remaining).")

      mergedCommands.subscribe:
        case Hello(sessionId) =>
          lock.synchronized(activeRecipients += sessionId)
          println(s"[Server] Registered session $sessionId.")
        case Send(sessionId, text) =>
          val recipients = lock.synchronized(activeRecipients.toList.sorted)
          println(s"[Server] Broadcasting from $sessionId to [${recipients.mkString(", ")}]: $text")
          recipients.foreach: recipient =>
            deliveryEmitter.emit(Delivery(recipient, sessionId, text))
        case Bye(sessionId) =>
          val removed = lock.synchronized(activeRecipients.remove(sessionId))
          if removed then println(s"[Server] Removed session $sessionId.")
          else println(s"[Server] Session $sessionId already absent.")

      Future:
        allStreamsClosed.await()
        println("[Server] All command streams closed. Closing deliveries.")
        deliveryEmitter.close()

      deliverySignal

    on[Client]:
      val sessionId = peerAddress.asInstanceOf[String]
      val incomingDeliveries = asLocal[Client, Server](deliveriesOnServer)
      val mailbox = new LinkedBlockingQueue[String]()
      val closed = new AtomicBoolean(false)

      incomingDeliveries.subscribe: delivery =>
        if delivery.to == sessionId then mailbox.put(s"${delivery.from}: ${delivery.text}")

      incomingDeliveries.onClose: () =>
        closed.set(true)

      var keepRunning = true
      while keepRunning do
        val next = mailbox.poll(awaitPollTimeoutMs, TimeUnit.MILLISECONDS)
        if next != null then println(s"[Client $sessionId] await -> $next")
        else if closed.get() && mailbox.isEmpty then keepRunning = false

      println(s"[Client $sessionId] no more messages.")
  end chatboxProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running HasteChatbox Section 2.3 port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val aliceNetwork = InMemoryNetwork[Client]("alice", broker)
    val bobNetwork = InMemoryNetwork[Client]("bob", broker)
    val carolNetwork = InMemoryNetwork[Client]("carol", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(chatboxProtocol) }
    val aliceFuture = Future { handleProgramForPeer[Client](aliceNetwork)(chatboxProtocol) }
    val bobFuture = Future { handleProgramForPeer[Client](bobNetwork)(chatboxProtocol) }
    val carolFuture = Future { handleProgramForPeer[Client](carolNetwork)(chatboxProtocol) }

    val all = Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("HasteChatbox done.")
end HasteChatbox
