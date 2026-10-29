package io.github.party

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration, DurationDouble }

import io.github.party.Collective
import io.github.party.CollectiveBuildingBlocks.{ distanceBetween, distanceTo, DistanceSensor }
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ CollectiveHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise

object Channel:
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

  def channel(source: Boolean, target: Boolean, width: Int)(using Network, Collective, Placement, DistanceSensor) =
    Collective[Node](0.5.second):
      distanceTo(source) + distanceTo(target) <= distanceBetween(source, target)

  def channelApp(using Network, Placement, Collective) =
    given DistanceSensor = NbrSensor.sensor(devicePositions)
    val channelSignal = channel(peerAddress == "node-1", peerAddress == "node-4", width = 1)
    val unitOf = on[Node]:
      val channels = take(channelSignal)
      channels.subscribe: isInChannel =>
        println(s"[$peerAddress] is in channel: $isInChannel")
    Thread.sleep(5000) // Keep the application running to observe outputs

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Collective) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, net.PeerAddress, V]
    program

  def main(args: Array[String]): Unit =
    println("Running Channel collective example...")
    val broker = InMemoryNetwork.broker()
    val node1 = InMemoryNetwork[Node]("node-1", broker)
    val node2 = InMemoryNetwork[Node]("node-2", broker)
    val node3 = InMemoryNetwork[Node]("node-3", broker)
    val node4 = InMemoryNetwork[Node]("node-4", broker)

    val futures = Seq(node1, node2, node3, node4).map: net =>
      Future:
        handleProgramForPeer[Node](net)(channelApp)
    Await.result(Future.sequence(futures), Duration.Inf)
end Channel
