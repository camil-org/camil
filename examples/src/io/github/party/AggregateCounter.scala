package io.github.party

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import io.github.party.Collective
import io.github.party.Collective.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.CollectiveHandler
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

object AggregateCounter:
  type Smartphone <: { type Tie <: Multiple[Smartphone] }

  private def counter(using Network, Placement, Collective) =
    val counter = Collective[Smartphone](1.second):
      rep(0)(_ + 1)
    on[Smartphone]:
      val signal = take(counter)
      signal.subscribe: value =>
        println(s"Device ${peerAddress} has count: $value")
    Thread.sleep(5_000) // Keep the program running for a while to observe the output

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Collective) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, net.PeerAddress, V]
    program

  def main(args: Array[String]): Unit =
    val broker = InMemoryNetwork.broker()
    val smartphone1 = InMemoryNetwork[Smartphone]("smartphone-1", broker)
    val smartphone2 = InMemoryNetwork[Smartphone]("smartphone-2", broker)

    val futures = Seq(smartphone1, smartphone2).map: net =>
      scala.concurrent.Future:
        handleProgramForPeer[Smartphone](net)(counter)
    scala.concurrent.Await.result(scala.concurrent.Future.sequence(futures), Duration.Inf)
end AggregateCounter
