package io.github.party

import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Collective
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveExampleSupport.*
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on

object Collection:
  type Node <: { type Tie <: Multiple[Node] }

  private def isContributor(peerId: String): Boolean =
    peerId == "node-2" || peerId == "node-4"

  private def collectionApp(using Network, Placement, Collective): Unit =
    given DistanceSensor = distanceSensor(Line4)
    val localPeer = peerAddress.asInstanceOf[String]
    val collection = Collective[Node](Line4.round):
      val isSource = localPeer == "node-1"
      val localContribution = if isContributor(localPeer) then 1.0 else 0.0
      val potential = distanceToWithinRadius(isSource)
      val collected = collectAlongPotential(potential, localContribution, 0.0)(_ + _)
      broadcastWithinRadius(isSource, collected)

    on[Node]:
      val value = awaitLatest(take(collection), convergenceWindow(Line4, propagationPhases = 6))
      println(f"[${peerAddress}] $value%.1f")

  def main(args: Array[String]): Unit =
    runCollectiveExample[Node](collectionApp)
end Collection
