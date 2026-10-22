package io.github.party

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.ChoreographyHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object RemoteFunction:
  type Client <: { type Tie <: Single[Server] }
  type Server <: { type Tie <: Single[Client] }

  private def call(input: Int on Client)(using Network, Placement, Choreography): Int on Client = Choreography:
    val inputAtServer = comm[Client, Server](input)
    val resultAtServer: Int on Server = on[Server]:
      take(inputAtServer) * 2
    comm[Server, Client](resultAtServer)

  def remoteFunctionProtocol(using Network, Placement, Choreography) = Choreography:
    val input = on[Client]:
      val value = 21
      println(s"[Client] calling remote function with $value")
      value
    val result = call(input)
    on[Client]:
      println(s"[Client] received ${take(result)}")

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    val broker = InMemoryNetwork.broker()
    val client = InMemoryNetwork[Client]("client", broker)
    val server = InMemoryNetwork[Server]("server", broker)

    val fc = Future { handleProgramForPeer[Client](client)(remoteFunctionProtocol) }
    val fs = Future { handleProgramForPeer[Server](server)(remoteFunctionProtocol) }

    Await.result(Future.sequence(Seq(fc, fs)), duration.Duration.Inf)
end RemoteFunction
