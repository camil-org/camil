package io.github.party

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ ChoreographyHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object PingPong:
  type Pinger <: { type Tie <: Single[Ponger] }
  type Ponger <: { type Tie <: Single[Pinger] }

  def pingPong(using Network, Placement, Choreography) = Choreography:
    var c = 0
    while c < 5 do
      println(s"--- [${summon[Network].peerAddress}] PingPong iteration $c ---")
      val onPinger = on[Pinger]:
        val message = s"Ping $c"
        println(s"Pinger sending message: $message")
        message
      val messageOnPonger = comm[Pinger, Ponger](onPinger)
      on[Ponger]:
        val msg = take(messageOnPonger)
        println(s"Pong received message: $msg")
      c += 1
      Thread.sleep(500) // Simulate some delay

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Choreography) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running PingPong choreography...")
    val broker = InMemoryNetwork.broker()
    val pingerNetwork = InMemoryNetwork[Pinger]("pinger", broker)
    val pongerNetwork = InMemoryNetwork[Ponger]("ponger", broker)

    val pingerFuture = Future { handleProgramForPeer[Pinger](pingerNetwork)(pingPong) }
    val pongerFuture = Future { handleProgramForPeer[Ponger](pongerNetwork)(pingPong) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(pingerFuture, pongerFuture))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end PingPong
