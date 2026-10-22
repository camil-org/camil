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

object CollectionIds:
  type Node <: { type Tie <: Multiple[Node] }

  private def isContributor(peerId: String): Boolean =
    peerId == "node-2" || peerId == "node-4"

  private def formatIds(ids: Set[Int]): String =
    ids.toList.sorted.mkString("{", ",", "}")

  private def collectionIdsApp(using Network, Placement, Collective): Unit =
    given DistanceSensor = distanceSensor(Line4)
    val localPeer = peerAddress.asInstanceOf[String]
    val collection = Collective[Node](Line4.round):
      val isSource = localPeer == "node-1"
      val localIds =
        if isContributor(localPeer) then Set(peerOrdinal(localPeer))
        else Set.empty[Int]
      val potential = distanceToWithinRadius(isSource)
      collectAlongPotential(potential, localIds, Set.empty[Int])(_ union _)

    on[Node]:
      val value = awaitLatest(take(collection), convergenceWindow(Line4, propagationPhases = 2))
      println(s"[${peerAddress}] ${formatIds(value)}")

  def main(args: Array[String]): Unit =
    runCollectiveExample[Node](collectionIdsApp)
end CollectionIds
