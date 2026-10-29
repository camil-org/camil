package io.github.party

import java.util.concurrent.CountDownLatch

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.GraffitiExampleSupport.*
import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ MultitierHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object GraffitiWebSockets:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class ClientStroke(from: String, draw: Draw)
  final case class Broadcast(from: String, draw: Draw, historySize: Int)

  def graffitiWebSocketProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val strokesOnClient = on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val script = websocketScripts.getOrElse(clientId, List.empty)
      val startupDelay = websocketStartupDelayMs.getOrElse(clientId, 500L)
      signalBuilder[ClientStroke]: emitter =>
        Thread.sleep(startupDelay)
        script.foreach: draw =>
          println(s"[Client $clientId] send -> ${renderDraw(draw)}")
          emitter.emit(ClientStroke(clientId, draw))
          Thread.sleep(betweenActionsDelayMs)

    val broadcastsOnServer = on[Server]:
      val strokeStreams = asLocalAll[Server, Client](strokesOnClient).toMap
      println(s"[Server] Tracking ${strokeStreams.size} Graffiti WebSocket client stream(s).")

      val mergedStrokes = Signal.merge(strokeStreams.values.toSeq)
      val (broadcastSignal, broadcastEmitter) = Signal.make[Broadcast]
      val allStreamsClosed = CountDownLatch(strokeStreams.size)
      val lock = new AnyRef
      var history = List.empty[Draw]

      strokeStreams.toSeq
        .sortBy(_._1.toString)
        .foreach:
          case (clientId, stream) =>
            stream.onClose: () =>
              allStreamsClosed.countDown()
              println(s"[Server] WebSocket stream closed for $clientId (${allStreamsClosed.getCount} remaining).")

      mergedStrokes.subscribe: stroke =>
        val historySize = lock.synchronized:
          history = history :+ stroke.draw
          history.size
        println(s"[Server] broadcast stroke from ${stroke.from} -> ${renderDraw(stroke.draw)}")
        broadcastEmitter.emit(Broadcast(stroke.from, stroke.draw, historySize))

      Future:
        allStreamsClosed.await()
        val finalHistory = lock.synchronized(history)
        println(s"[Server] Final websocket canvas -> ${renderHistory(finalHistory)}")
        broadcastEmitter.close()

      broadcastSignal

    on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val incomingBroadcasts = asLocal[Client, Server](broadcastsOnServer)
      val done = CountDownLatch(1)
      val lock = new AnyRef
      var rendered = List.empty[Draw]

      incomingBroadcasts.subscribe: broadcast =>
        val renderedCount = lock.synchronized:
          rendered = rendered :+ broadcast.draw
          rendered.size
        println(
          s"[Client $clientId] render <- ${broadcast.from}: ${renderDraw(broadcast.draw)} (historySize=${broadcast.historySize}, localCount=$renderedCount)",
        )

      incomingBroadcasts.onClose: () =>
        val snapshot = lock.synchronized(rendered)
        println(s"[Client $clientId] websocket stream closed with ${snapshot.size} rendered stroke(s).")
        done.countDown()

      done.await()
  end graffitiWebSocketProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running GraffitiWebSockets scripted port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val aliceNetwork = InMemoryNetwork[Client]("alice", broker)
    val bobNetwork = InMemoryNetwork[Client]("bob", broker)
    val carolNetwork = InMemoryNetwork[Client]("carol", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(graffitiWebSocketProtocol) }
    val aliceFuture = Future { handleProgramForPeer[Client](aliceNetwork)(graffitiWebSocketProtocol) }
    val bobFuture = Future { handleProgramForPeer[Client](bobNetwork)(graffitiWebSocketProtocol) }
    val carolFuture = Future { handleProgramForPeer[Client](carolNetwork)(graffitiWebSocketProtocol) }

    val all = Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("GraffitiWebSockets done.")
end GraffitiWebSockets
