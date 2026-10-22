package io.github.party

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.Collective
import io.github.party.Collective.*
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveBuildingBlocks.DistanceSensor.nbrRange
import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.ChoreographyHandler
import io.github.party.handlers.CollectiveHandler
import io.github.party.handlers.MultitierHandler
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
import io.github.party.placement.PlacementType.place
import io.github.party.raise.Raise
import io.github.party.NbrSensor.sensor
import scala.caps.Stateful
import scala.caps.SharedCapability

object SmartMobility:
  type Cloud <: { type Tie <: Single[Edge] & Multiple[Vehicle] }
  type Edge <: { type Tie <: Single[Cloud] & Multiple[Vehicle] }
  type Vehicle <: { type Tie <: Single[Cloud] & Single[Edge] & Multiple[Vehicle] }

  final case class VehicleRegistration(vehicleId: String, vehicleType: String, entryGate: String)
  final case class VehicleToken(value: String)
  final case class VehicleTelemetry(
      vehicleId: String,
      currentSegment: String,
      position: (Double, Double),
      speedKmh: Double,
      hardBrake: Boolean,
      destination: String,
  )
  final case class TrafficProbe(
      vehicleId: String,
      segmentId: String,
      nearbyVehicles: Int,
      avgNearbySpeedKmh: Double,
      hazardDetected: Boolean,
      localSpeedKmh: Double,
  )
  final case class SegmentStatus(
      segmentId: String,
      intersectionId: String,
      vehicleIds: List[String],
      avgSpeedKmh: Double,
      hazardDetected: Boolean,
      congested: Boolean,
  )
  final case class TrafficLightPlan(
      intersectionId: String,
      mode: String,
      prioritySegment: Option[String],
      managedSegments: List[String],
      reason: String,
  )
  final case class RouteOption(label: String, segments: List[String], baseMinutes: Int)
  final case class RouteRequest(
      vehicleId: String,
      token: VehicleToken,
      originSegment: String,
      destination: String,
      options: List[RouteOption],
  )
  final case class RouteRecommendation(
      vehicleId: String,
      selected: RouteOption,
      score: Int,
      rationale: String,
      authenticated: Boolean,
  )
  final case class RoutingSnapshot(
      segmentStatuses: Map[String, SegmentStatus],
      lightPlans: Map[String, TrafficLightPlan],
  )

  private val ProbeRadius = 1.0
  private val ProbeRound = 250.millis
  private val ProbeWarmupMillis = 900L

  private val vehicleRegistrations = Map(
    "vehicle-1" -> VehicleRegistration("vehicle-1", "sedan", "north-gate"),
    "vehicle-2" -> VehicleRegistration("vehicle-2", "delivery-van", "north-gate"),
    "vehicle-3" -> VehicleRegistration("vehicle-3", "sedan", "west-gate"),
    "vehicle-4" -> VehicleRegistration("vehicle-4", "taxi", "west-gate"),
  )

  private val vehicleTelemetries = Map(
    "vehicle-1" -> VehicleTelemetry("vehicle-1", "central-corridor", (0.0, 0.0), 16.0, false, "downtown"),
    "vehicle-2" -> VehicleTelemetry("vehicle-2", "central-corridor", (0.6, 0.1), 8.0, true, "downtown"),
    "vehicle-3" -> VehicleTelemetry("vehicle-3", "school-avenue", (3.0, 0.0), 14.0, false, "campus"),
    "vehicle-4" -> VehicleTelemetry("vehicle-4", "school-avenue", (3.7, 0.2), 15.0, false, "campus"),
  )

  private val routeCatalog = Map(
    "vehicle-1" -> List(
      RouteOption("central-direct", List("central-corridor", "downtown-spur"), 8),
      RouteOption("river-bypass", List("river-bypass", "downtown-spur"), 11),
    ),
    "vehicle-2" -> List(
      RouteOption("central-direct", List("central-corridor", "downtown-spur"), 9),
      RouteOption("river-bypass", List("river-bypass", "downtown-spur"), 12),
    ),
    "vehicle-3" -> List(
      RouteOption("school-main", List("school-avenue", "campus-link"), 9),
      RouteOption("orbital-road", List("orbital-road", "campus-link"), 12),
    ),
    "vehicle-4" -> List(
      RouteOption("school-main", List("school-avenue", "campus-link"), 10),
      RouteOption("orbital-road", List("orbital-road", "campus-link"), 12),
    ),
  )

  private val segmentIntersections = Map(
    "central-corridor" -> "junction-central",
    "school-avenue" -> "junction-school",
  )

  private val managedSegmentsByIntersection = Map(
    "junction-central" -> List("central-corridor"),
    "junction-school" -> List("school-avenue"),
  )

  private final class SmartMobilityState extends Stateful, SharedCapability:
    var registeredVehicles: Map[String, VehicleToken] on Cloud = place(Map.empty)
    var vehicleToken: Option[VehicleToken] on Vehicle = place(None)
    var latestProbe: Option[TrafficProbe] on Vehicle = place(None)
    var latestSegmentStatuses: Map[String, SegmentStatus] on Edge = place(Map.empty)
    var latestLightPlans: Map[String, TrafficLightPlan] on Edge = place(Map.empty)
    var latestRoutingSnapshot: Option[RoutingSnapshot] on Edge = place(None)
    var latestRecommendations: Map[String, RouteRecommendation] on Cloud = place(Map.empty)

  private def registrationFor(peerId: String): VehicleRegistration =
    vehicleRegistrations.getOrElse(peerId, VehicleRegistration(peerId, "unknown", "control-gate"))

  private def telemetryFor(peerId: String): VehicleTelemetry =
    vehicleTelemetries.getOrElse(
      peerId,
      VehicleTelemetry(peerId, "control-segment", (999.0, 999.0), 0.0, false, "control-room"),
    )

  private def currentVehicleId(using vm: VM): String = vm.deviceId.toString

  private def routesFor(peerId: String): List[RouteOption] =
    routeCatalog.getOrElse(peerId, List(RouteOption("hold-position", Nil, 999)))

  private def segmentIntersection(segmentId: String): String =
    segmentIntersections.getOrElse(segmentId, s"junction-$segmentId")

  private def fmtSpeed(speed: Double): String = f"$speed%.1f km/h"

  private def formatProbe(probe: TrafficProbe): String =
    s"segment=${probe.segmentId}, nearby=${probe.nearbyVehicles}, avgSpeed=${fmtSpeed(probe.avgNearbySpeedKmh)}, " +
      s"hazard=${probe.hazardDetected}, localSpeed=${fmtSpeed(probe.localSpeedKmh)}"

  private def formatSegmentStatus(status: SegmentStatus): String =
    val vehicles = status.vehicleIds.mkString(", ")
    s"${status.segmentId}@${status.intersectionId}: vehicles=[$vehicles], avgSpeed=${fmtSpeed(status.avgSpeedKmh)}, " +
      s"hazard=${status.hazardDetected}, congested=${status.congested}"

  private def formatLightPlan(plan: TrafficLightPlan): String =
    val priority = plan.prioritySegment.getOrElse("none")
    s"${plan.intersectionId}: mode=${plan.mode}, priority=$priority, reason=${plan.reason}"

  private def formatRecommendation(recommendation: RouteRecommendation): String =
    val route = recommendation.selected.segments.mkString(" -> ")
    s"${recommendation.vehicleId}: route=${recommendation.selected.label} [$route], score=${recommendation.score}, " +
      s"authenticated=${recommendation.authenticated}, rationale=${recommendation.rationale}"

  private def buildSegmentStatuses(probes: List[TrafficProbe]): Map[String, SegmentStatus] =
    probes
      .groupBy(_.segmentId)
      .view
      .mapValues: group =>
        val avgSpeed = group.map(_.localSpeedKmh).sum / group.size.toDouble
        SegmentStatus(
          segmentId = group.head.segmentId,
          intersectionId = segmentIntersection(group.head.segmentId),
          vehicleIds = group.map(_.vehicleId).sorted,
          avgSpeedKmh = avgSpeed,
          hazardDetected = group.exists(_.hazardDetected),
          congested = group.size >= 2 && avgSpeed <= 20.0,
        )
      .toMap

  private def buildLightPlans(segmentStatuses: Map[String, SegmentStatus]): Map[String, TrafficLightPlan] =
    managedSegmentsByIntersection.map:
      case (intersectionId, managedSegments) =>
        val statuses = managedSegments.flatMap(segmentStatuses.get)
        val hazardStatus = statuses.find(_.hazardDetected)
        val congestedStatus = statuses.filter(_.congested).sortBy(_.avgSpeedKmh).headOption
        val plan =
          hazardStatus match
            case Some(status) =>
              TrafficLightPlan(
                intersectionId = intersectionId,
                mode = "caution",
                prioritySegment = Some(status.segmentId),
                managedSegments = managedSegments,
                reason = s"hazard detected on ${status.segmentId}",
              )
            case None =>
              congestedStatus match
                case Some(status) =>
                  TrafficLightPlan(
                    intersectionId = intersectionId,
                    mode = "extend-green",
                    prioritySegment = Some(status.segmentId),
                    managedSegments = managedSegments,
                    reason = s"congestion relief for ${status.segmentId}",
                  )
                case None =>
                  TrafficLightPlan(
                    intersectionId = intersectionId,
                    mode = "normal-cycle",
                    prioritySegment = None,
                    managedSegments = managedSegments,
                    reason = "balanced flow",
                  )
        intersectionId -> plan

  private def segmentPenalty(segment: String, snapshot: RoutingSnapshot): (Int, List[String]) =
    val statusPenalty = snapshot.segmentStatuses
      .get(segment)
      .toList
      .flatMap: status =>
        List(
          Option.when(status.hazardDetected)(18 -> s"$segment hazard +18"),
          Option.when(status.congested)(4 -> s"$segment congestion +4"),
        ).flatten
    val planPenalty = snapshot.lightPlans.values
      .find(_.managedSegments.contains(segment))
      .toList
      .flatMap: plan =>
        plan.mode match
          case "caution" => List(6 -> s"${plan.intersectionId} caution +6")
          case "extend-green" if plan.prioritySegment.contains(segment) => List(-2 -> s"${plan.intersectionId} green wave -2")
          case "extend-green" => List(2 -> s"${plan.intersectionId} cross-flow +2")
          case _ => Nil
    val allPenalties = statusPenalty ++ planPenalty
    val total = allPenalties.map(_._1).sum
    val reasons = allPenalties.map(_._2)
    (total, reasons)
  end segmentPenalty

  private def recommendRoute(request: RouteRequest, snapshot: RoutingSnapshot): RouteRecommendation =
    val scoredOptions = request.options.map: option =>
      val penalties = option.segments.map(segmentPenalty(_, snapshot))
      val extra = penalties.map(_._1).sum
      val reasons = penalties.flatMap(_._2)
      val score = option.baseMinutes + extra
      val reasonText =
        if reasons.isEmpty then s"base=${option.baseMinutes}, no traffic penalties"
        else s"base=${option.baseMinutes}, ${reasons.mkString(", ")}"
      (option, score, reasonText)
    val (selected, score, reasonText) = scoredOptions.minBy(_._2)
    RouteRecommendation(
      vehicleId = request.vehicleId,
      selected = selected,
      score = score,
      rationale = s"$reasonText -> choose ${selected.label}",
      authenticated = true,
    )

  private def registrationPhase(state: SmartMobilityState)(using Network, Placement, Multitier): Unit = Multitier:
    val registrationAtVehicle = on[Vehicle]:
      val registration = registrationFor(peerAddress.asInstanceOf[String])
      println(s"[${registration.vehicleId}] entering the city via ${registration.entryGate}")
      registration

    val issuedTokensAtCloud = on[Cloud]:
      val registrations = asLocalAll[Cloud, Vehicle](registrationAtVehicle).values.toList.sortBy(_.vehicleId)
      val issuedTokens = registrations.map: registration =>
        registration.vehicleId -> VehicleToken(s"token-${registration.vehicleId}")
      val registry = issuedTokens.toMap
      state.registeredVehicles = place(registry)
      println(s"[cloud] registered ${registry.size} vehicle(s)")
      registry.toList
        .sortBy(_._1)
        .foreach: (vehicleId, token) =>
          println(s"[cloud] issued ${token.value} to $vehicleId")
      registry

    on[Vehicle]:
      val vehicleId = peerAddress.asInstanceOf[String]
      val token = asLocal[Vehicle, Cloud](issuedTokensAtCloud)(vehicleId)
      state.vehicleToken = place(Some(token))
      println(s"[$vehicleId] received credential ${token.value}")

  private def trafficSensingPhase(state: SmartMobilityState)(using Network, Placement, Collective): Unit =
    given DistanceSensor = new DistanceSensor:
      def nbrRange(using Collective, VM): Field[Double] =
        val localPosition = telemetryFor(currentVehicleId).position
        nbr(localPosition).map: remotePosition =>
          val dx = localPosition._1 - remotePosition._1
          val dy = localPosition._2 - remotePosition._2
          math.sqrt((dx * dx) + (dy * dy))

    val trafficSignal = Collective[Vehicle](ProbeRound):
      val localTelemetry = telemetryFor(currentVehicleId)
      val neighboringDistances = nbrRange
      val nearbySpeedContrib = neighboringDistances.combine(nbr(localTelemetry.speedKmh)): (distance, speed) =>
        if distance <= ProbeRadius then speed else 0.0
      val nearbyBrakeContrib = neighboringDistances.combine(nbr(localTelemetry.hardBrake)): (distance, hardBrake) =>
        distance <= ProbeRadius && hardBrake
      val closeNeighborCount = neighboringDistances.withoutSelf.values.count(_ <= ProbeRadius)
      val nearbyVehicles = closeNeighborCount + 1
      val avgNearbySpeed =
        (localTelemetry.speedKmh + nearbySpeedContrib.withoutSelf.values.sum) / nearbyVehicles.toDouble
      val hazardDetected =
        localTelemetry.hardBrake || nearbyBrakeContrib.withoutSelf.values.exists(identity)
      TrafficProbe(
        vehicleId = localTelemetry.vehicleId,
        segmentId = localTelemetry.currentSegment,
        nearbyVehicles = nearbyVehicles,
        avgNearbySpeedKmh = avgNearbySpeed,
        hazardDetected = hazardDetected,
        localSpeedKmh = localTelemetry.speedKmh,
      )

    on[Vehicle]:
      take(trafficSignal).subscribe: probe =>
        state.latestProbe = place(Some(probe))
        println(s"[${probe.vehicleId}] probe -> ${formatProbe(probe)}")
  end trafficSensingPhase

  private def signalCoordinationPhase(state: SmartMobilityState)(using Network, Placement, Multitier): Unit = Multitier:
    val latestProbeAtVehicle = on[Vehicle]:
      take(state.latestProbe)

    on[Edge]:
      val probes = asLocalAll[Edge, Vehicle](latestProbeAtVehicle).values.toList.flatten.sortBy(_.vehicleId)
      val segmentStatuses = buildSegmentStatuses(probes)
      val lightPlans = buildLightPlans(segmentStatuses)
      val snapshot = RoutingSnapshot(segmentStatuses, lightPlans)
      state.latestSegmentStatuses = place(segmentStatuses)
      state.latestLightPlans = place(lightPlans)
      state.latestRoutingSnapshot = place(Some(snapshot))

      println("\n[edge] segment status summary")
      segmentStatuses.values.toList
        .sortBy(_.segmentId)
        .foreach: status =>
          println(s" - ${formatSegmentStatus(status)}")

      println("[edge] traffic light plans")
      lightPlans.values.toList
        .sortBy(_.intersectionId)
        .foreach: plan =>
          println(s" - ${formatLightPlan(plan)}")

  private def routeRecommendationPhase(state: SmartMobilityState)(using Network, Placement, Choreography): Unit = Choreography:
    val routeRequestAtVehicle = on[Vehicle]:
      val vehicleId = peerAddress.asInstanceOf[String]
      val localTelemetry = telemetryFor(vehicleId)
      val request = RouteRequest(
        vehicleId = vehicleId,
        token = take(state.vehicleToken).getOrElse(VehicleToken("missing-token")),
        originSegment = localTelemetry.currentSegment,
        destination = localTelemetry.destination,
        options = routesFor(vehicleId),
      )
      println(
        s"[$vehicleId] route request -> origin=${request.originSegment}, destination=${request.destination}, " +
          s"options=${request.options.map(_.label).mkString(", ")}",
      )
      request

    val gatheredRequestsAtEdge = gather[Vehicle, Edge](routeRequestAtVehicle)

    val batchAtEdge = on[Edge]:
      val requests = take(gatheredRequestsAtEdge).values.toList.sortBy(_.vehicleId).map(request => request.vehicleId -> request).toMap
      val snapshot = RoutingSnapshot(take(state.latestSegmentStatuses), take(state.latestLightPlans))
      state.latestRoutingSnapshot = place(Some(snapshot))
      println(s"[edge] forwarding ${requests.size} route request(s) with current traffic snapshot")
      (requests, snapshot)

    val batchAtCloud = comm[Edge, Cloud](batchAtEdge)

    val recommendationsAtCloud = on[Cloud]:
      val (requests, snapshot) = take(batchAtCloud)
      val registry = take(state.registeredVehicles)
      val recommendations = requests.values.toList
        .sortBy(_.vehicleId)
        .map: request =>
          val authenticated = registry.get(request.vehicleId).contains(request.token)
          val recommendation =
            if authenticated then recommendRoute(request, snapshot)
            else
              RouteRecommendation(
                vehicleId = request.vehicleId,
                selected = request.options.head,
                score = 9999,
                rationale = "authentication failed",
                authenticated = false,
              )
          val decision = if authenticated then "accepted" else "rejected"
          println(s"[cloud] $decision ${request.vehicleId} -> ${formatRecommendation(recommendation)}")
          request.vehicleId -> recommendation
      val result = recommendations.toMap
      state.latestRecommendations = place(result)
      result

    val recommendationsAtEdge = comm[Cloud, Edge](recommendationsAtCloud)
    val recommendationsAtVehicle = multicast[Edge, Vehicle](recommendationsAtEdge)

    on[Vehicle]:
      val vehicleId = peerAddress.asInstanceOf[String]
      val recommendation = take(recommendationsAtVehicle)(vehicleId)
      println(s"[$vehicleId] recommended route -> ${formatRecommendation(recommendation)}")

  private def finalSummary(state: SmartMobilityState)(using Network, Placement): Unit =
    on[Edge]:
      println("\n=== Smart Mobility Edge Summary ===")
      take(state.latestRoutingSnapshot) match
        case Some(snapshot) =>
          snapshot.segmentStatuses.values.toList
            .sortBy(_.segmentId)
            .foreach: status =>
              println(s" - ${formatSegmentStatus(status)}")
          snapshot.lightPlans.values.toList
            .sortBy(_.intersectionId)
            .foreach: plan =>
              println(s" - ${formatLightPlan(plan)}")
        case None =>
          println(" - no routing snapshot available")

    on[Cloud]:
      println("\n=== Smart Mobility Cloud Summary ===")
      take(state.registeredVehicles).toList
        .sortBy(_._1)
        .foreach: (vehicleId, token) =>
          println(s" - token $vehicleId -> ${token.value}")
      take(state.latestRecommendations).values.toList
        .sortBy(_.vehicleId)
        .foreach: recommendation =>
          println(s" - ${formatRecommendation(recommendation)}")
  end finalSummary

  private def smartMobilityApp(using Network, Placement, Multitier, Choreography, Collective): Unit =
    val state = SmartMobilityState()
    registrationPhase(state)
    trafficSensingPhase(state)
    Thread.sleep(ProbeWarmupMillis)
    signalCoordinationPhase(state)
    routeRecommendationPhase(state)
    finalSummary(state)

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography, Collective, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, String, V]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running SmartMobility integrated example...")

    val broker = InMemoryNetwork.broker()
    val cloudNetwork = InMemoryNetwork[Cloud]("cloud", broker)
    val edgeNetwork = InMemoryNetwork[Edge]("edge", broker)
    val vehicle1Network = InMemoryNetwork[Vehicle]("vehicle-1", broker)
    val vehicle2Network = InMemoryNetwork[Vehicle]("vehicle-2", broker)
    val vehicle3Network = InMemoryNetwork[Vehicle]("vehicle-3", broker)
    val vehicle4Network = InMemoryNetwork[Vehicle]("vehicle-4", broker)

    val futures = Seq(
      Future(handleProgramForPeer[Cloud](cloudNetwork)(smartMobilityApp)),
      Future(handleProgramForPeer[Edge](edgeNetwork)(smartMobilityApp)),
      Future(handleProgramForPeer[Vehicle](vehicle1Network)(smartMobilityApp)),
      Future(handleProgramForPeer[Vehicle](vehicle2Network)(smartMobilityApp)),
      Future(handleProgramForPeer[Vehicle](vehicle3Network)(smartMobilityApp)),
      Future(handleProgramForPeer[Vehicle](vehicle4Network)(smartMobilityApp)),
    )

    Await.result(Future.sequence(futures), Duration.Inf)
  end main
end SmartMobility
