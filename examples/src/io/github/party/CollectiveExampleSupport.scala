package io.github.party

import scala.caps.unsafe.unsafeAssumePure
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import io.github.party.Collective
import io.github.party.Collective.*
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveBuildingBlocks.G
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.CollectiveHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType
import io.github.party.raise.Raise
import io.github.party.signal.Signal

object CollectiveExampleSupport:
  final case class StaticTopology(
      positions: Map[String, (Double, Double)],
      communicationRadius: Double,
      round: FiniteDuration,
      observationWindow: FiniteDuration,
  ):
    val peerIds: List[String] = positions.keys.toList.sorted

  val Line4 = StaticTopology(
    positions = Map(
      "node-1" -> (0.0, 0.0),
      "node-2" -> (1.0, 0.0),
      "node-3" -> (2.0, 0.0),
      "node-4" -> (3.0, 0.0),
    ),
    communicationRadius = 1.1,
    round = 500.millis,
    observationWindow = 2500.millis,
  )

  def peerOrdinal(peerId: String): Int =
    peerId.stripPrefix("node-").toInt

  def distanceSensor(topology: StaticTopology = Line4)(using Collective): DistanceSensor =
    NbrSensor.sensor(topology.positions)

  def awaitLatest[V](signal: Signal[V], observationWindow: FiniteDuration): V =
    @volatile var latest: Option[V] = None
    val subscription = signal.subscribe(value => latest = Some(value))
    Thread.sleep(observationWindow.toMillis)
    subscription.cancel()
    latest.getOrElse(throw new IllegalStateException("Collective example did not emit a value"))

  def convergenceWindow(
      topology: StaticTopology = Line4,
      propagationPhases: Int = 1,
  ): FiniteDuration =
    topology.observationWindow + (topology.round * ((topology.peerIds.size - 1L) * propagationPhases.toLong))

  def runCollectiveExample[P <: Peer: PeerTag](
      program: (Network, Placement, Collective) ?=> Unit,
      topology: StaticTopology = Line4,
  ): Unit =
    val broker = InMemoryNetwork.broker()
    val futures = topology.peerIds.map { peerId =>
      val net = InMemoryNetwork[P](peerId, broker)
      Future(handleProgramForPeer[P](net)(program))
    }
    Await.result(Future.sequence(futures), Duration.Inf)

  def distanceToWithinRadius(
      source: Boolean,
      topology: StaticTopology = Line4,
  )(using Collective, VM, DistanceSensor): Double =
    rep(Double.PositiveInfinity): distance =>
      mux(source) {
        0.0
      } {
        nbr(distance)
          .combine(inRangeDistances(topology))(_ + _)
          .withoutSelf
          .values
          .minOption
          .getOrElse(Double.PositiveInfinity)
      }

  def broadcastWithinRadius[V](
      source: Boolean,
      value: V,
      topology: StaticTopology = Line4,
  )(using Collective, VM, DistanceSensor): V =
    G(source, value, identity, () => inRangeDistances(topology))

  def collectAlongPotential[V](
      potential: Double,
      local: V,
      zero: V,
      topology: StaticTopology = Line4,
  )(combine: (V, V) => V)(using Collective, VM, DistanceSensor): V =
    val pureCombine = unsafeAssumePure(combine)
    rep(local): collected =>
      val localPeer = summon[VM].deviceId.toString
      val parent = parentWithinRadius(potential, topology)
      val contributions = nbr(collected).combine(nbr(parent)): (value, selectedParent) =>
        if selectedParent == localPeer then value else zero
      pureCombine(local, contributions.withoutSelf.values.foldLeft(zero)(pureCombine))

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)(
      program: (Network, Placement, Collective) ?=> Unit,
  ): Unit =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, net.PeerAddress, Unit]
    program

  private def inRangeDistances(
      topology: StaticTopology,
  )(using Collective, VM, DistanceSensor): Field[Double] =
    DistanceSensor.nbrRange.map: distance =>
      if distance <= topology.communicationRadius then distance else Double.PositiveInfinity

  private def parentWithinRadius(
      potential: Double,
      topology: StaticTopology,
  )(using Collective, VM, DistanceSensor): String =
    val localPeer = summon[VM].deviceId.toString
    val neighbors = nbr(potential)
      .combine(inRangeDistances(topology))((neighborPotential, distance) => (neighborPotential, distance))
      .withoutSelf
      .asInstanceOf[Map[Any, (Double, Double)]]
    val candidates = neighbors.iterator
      .flatMap: (neighborId, candidate) =>
        val (neighborPotential, distance) = candidate
        if distance.isFinite then Some((neighborPotential, peerOrdinal(neighborId.toString), neighborId.toString))
        else None
      .toList
    candidates.minOption match
      case Some((neighborPotential, _, neighborId)) if neighborPotential < potential => neighborId
      case _ => localPeer
end CollectiveExampleSupport
