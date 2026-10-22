package io.github.party

import io.github.party.peers.Peers.Cardinality.*
import io.github.party.network.Network
import io.github.party.network.NetworkError
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.*
import io.github.party.Multitier.*
import io.github.party.peers.Peers.*
import io.github.party.raise.Raise
import io.github.party.handlers.*
import io.github.party.distributed.InMemoryNetwork
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.*

object ChatApplication:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  def chat(using Network, Placement, Multitier) = Multitier:
    val message = on[Client] { scala.util.Random.alphanumeric.take(10).mkString }
    on[Server]:
      val messages = asLocalAll(message)
      messages.values.foreach { msg => println(s"[Server] Received message: $msg") }

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Multitier) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running ChatApplication...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val client1Network = InMemoryNetwork[Client]("client1", broker)
    val client2Network = InMemoryNetwork[Client]("client2", broker)

    val clientFuture = Future { handleProgramForPeer[Server](serverNetwork)(chat) }
    val primaryFuture = Future { handleProgramForPeer[Client](client1Network)(chat) }
    val secondaryFuture = Future { handleProgramForPeer[Client](client2Network)(chat) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(clientFuture, primaryFuture, secondaryFuture))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end ChatApplication
