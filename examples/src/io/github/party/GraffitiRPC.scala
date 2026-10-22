package io.github.party

import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.github.party.GraffitiExampleSupport.*
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
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object GraffitiRPC:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class ClientAction(from: String, action: RpcAction)
  final case class Reply(to: String, message: String)

  private def renderAction(action: RpcAction): String =
    action match
      case Add(draw) => s"Add(${renderDraw(draw)})"
      case ClearCanvas => "ClearCanvas"

  def graffitiRpcProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val actionsOnClient = on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val script = rpcScripts.getOrElse(clientId, List.empty)
      val startupDelay = rpcStartupDelayMs.getOrElse(clientId, 500L)
      signalBuilder[ClientAction]: emitter =>
        Thread.sleep(startupDelay)
        script.foreach: action =>
          println(s"[Client $clientId] RPC -> ${renderAction(action)}")
          emitter.emit(ClientAction(clientId, action))
          Thread.sleep(betweenActionsDelayMs)

    val repliesOnServer = on[Server]:
      val actionStreams = asLocalAll[Server, Client](actionsOnClient).toMap
      println(s"[Server] Tracking ${actionStreams.size} Graffiti RPC client stream(s).")

      val mergedActions = Signal.merge(actionStreams.values.toSeq)
      val (replySignal, replyEmitter) = Signal.make[Reply]
      val allStreamsClosed = CountDownLatch(actionStreams.size)
      val lock = new AnyRef
      var history = List.empty[Draw]

      actionStreams.toSeq
        .sortBy(_._1.toString)
        .foreach:
          case (clientId, stream) =>
            stream.onClose: () =>
              allStreamsClosed.countDown()
              println(s"[Server] RPC stream closed for $clientId (${allStreamsClosed.getCount} remaining).")

      mergedActions.subscribe:
        case ClientAction(clientId, Add(draw)) =>
          val historySize = lock.synchronized:
            history = history :+ draw
            history.size
          println(s"[Server] $clientId accepted stroke -> ${renderDraw(draw)}")
          replyEmitter.emit(Reply(clientId, s"Stored stroke #$historySize."))
        case ClientAction(clientId, ClearCanvas) =>
          lock.synchronized:
            history = List.empty
          println(s"[Server] $clientId cleared the canvas.")
          replyEmitter.emit(Reply(clientId, "Canvas cleared."))

      Future:
        allStreamsClosed.await()
        val finalHistory = lock.synchronized(history)
        println(s"[Server] Final persisted canvas -> ${renderHistory(finalHistory)}")
        replyEmitter.close()

      replySignal

    on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val incomingReplies = asLocal[Client, Server](repliesOnServer)
      val done = CountDownLatch(1)

      incomingReplies.subscribe: reply =>
        if reply.to == clientId then println(s"[Client $clientId] ack -> ${reply.message}")

      incomingReplies.onClose: () =>
        println(s"[Client $clientId] RPC reply stream closed.")
        done.countDown()

      done.await()
  end graffitiRpcProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running GraffitiRPC scripted port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val aliceNetwork = InMemoryNetwork[Client]("alice", broker)
    val bobNetwork = InMemoryNetwork[Client]("bob", broker)
    val carolNetwork = InMemoryNetwork[Client]("carol", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(graffitiRpcProtocol) }
    val aliceFuture = Future { handleProgramForPeer[Client](aliceNetwork)(graffitiRpcProtocol) }
    val bobFuture = Future { handleProgramForPeer[Client](bobNetwork)(graffitiRpcProtocol) }
    val carolFuture = Future { handleProgramForPeer[Client](carolNetwork)(graffitiRpcProtocol) }

    val all = Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("GraffitiRPC done.")
end GraffitiRPC
