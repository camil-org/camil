package io.github.party

import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Collective
import io.github.party.Collective.*
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveBuildingBlocks.DistanceSensor.nbrRange
import io.github.party.CollectiveExampleSupport.*
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on

object GradientWithExplicitFields:
  type Node <: { type Tie <: Multiple[Node] }

  private def gradientApp(using Network, Placement, Collective): Unit =
    given DistanceSensor = distanceSensor(Line4)
    val localPeer = peerAddress.asInstanceOf[String]
    val gradient = Collective[Node](Line4.round):
      val isSource = localPeer == "node-1"
      rep(Double.PositiveInfinity): distance =>
        mux(isSource) {
          0.0
        } {
          val inRangeMetrics = nbrRange.map: step =>
            if step <= Line4.communicationRadius then step else Double.PositiveInfinity
          nbr(distance)
            .combine(inRangeMetrics)(_ + _)
            .withoutSelf
            .values
            .minOption
            .getOrElse(Double.PositiveInfinity)
        }

    on[Node]:
      val value = awaitLatest(take(gradient), convergenceWindow(Line4))
      println(f"[${peerAddress}] $value%.1f")
  end gradientApp

  def main(args: Array[String]): Unit =
    runCollectiveExample[Node](gradientApp)
end GradientWithExplicitFields
