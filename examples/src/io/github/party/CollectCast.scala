package io.github.party

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationDouble

import io.github.party.Collective
import io.github.party.Collective.nbr
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
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.CollectiveBuildingBlocks.*
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveExampleSupport.awaitLatest

object CollectCast:
  type Node <: { type Tie <: Multiple[Node] }

  /* 
    node-1 (0,0) ------- node-2 (1.5, 0)
      |          \       /
    node-3 (0,1) - node-4 (1,1)
   */
  private val devicePositions = Map(
      "node-1" -> (0.0, 0.0),
      "node-2" -> (1.5, 0.0),
      "node-3" -> (0.0, 1.0),
      "node-4" -> (1.0, 1.0),
  )

  /** Each device flags `sense`; values flow down the gradient towards `sink`, which ends up with the total count. */
  def collectCast(sink: Boolean, sense: Boolean)(using Network, Collective, PlacementType^, DistanceSensor) =
    Collective[Node](0.5.second):
      val potential = nbr(distanceTo(sink))
      C(potential, local = if sense then 1 else 0, combine = _ + _, zero = 0)

  def app(using Network, Placement, Collective, DistanceSensor) =
    val countSignal = collectCast(sink = peerAddress == "node-1", sense = peerAddress == "node-4")
    val unitOf = on[Node]:
      val value = awaitLatest(take(countSignal), 2500.millis)
      println(s"[${peerAddress}] $value")

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
    program: (Network, Placement, Collective, DistanceSensor) ?=> V
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, net.PeerAddress, V]
    given DistanceSensor = NbrSensor.fromDevicePositions(devicePositions)
    program

  def main(args: Array[String]): Unit =
    println("Running Channel collective example...")
    val broker = InMemoryNetwork.broker()
    val node1 = InMemoryNetwork[Node]("node-1", broker)
    val node2 = InMemoryNetwork[Node]("node-2", broker)
    val node3 = InMemoryNetwork[Node]("node-3", broker)
    val node4 = InMemoryNetwork[Node]("node-4", broker)

    val futures = Seq(node1, node2, node3, node4).map { net =>
      scala.concurrent.Future:
        handleProgramForPeer[Node](net)(app)
    }
    scala.concurrent.Await.result(scala.concurrent.Future.sequence(futures), Duration.Inf)
