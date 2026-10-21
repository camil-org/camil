package io.github.party.nebula

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
import io.github.party.CollectiveBuildingBlocks.G
import io.github.party.Field
import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.VM
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.ChoreographyHandler
import io.github.party.handlers.CollectiveHandler
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.nebula.NebulaDomain.*
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
import scala.caps.Stateful

object Nebula:
  type Coordinator <: { type Tie <: Multiple[Participant] & Single[Dashboard] }
  type Participant <: { type Tie <: Single[Coordinator] & Single[Dashboard] & Multiple[Participant] }
  type Dashboard <: { type Tie <: Single[Coordinator] & Multiple[Participant] }

  private val AggregationSeed = "trainer-a"
  private val InfinityDistance = 1e9
  private val CloseNeighborThreshold = 1.6
  private val FederationRadius = 2.0
  private val StabilityThreshold = 0.35
  private val RoundInterval = 400.millis
  private val TopologyWarmupMillis = 1500L
  private val TotalRounds = 3

  private val initialModel = Model(Vector(0.90, -0.30, 0.15))

  private val datasets = Map(
    "trainer-a" -> LocalDataset(24, Vector(0.30, -0.12, 0.08), (0.0, 0.0)),
    "trainer-b" -> LocalDataset(36, Vector(0.22, -0.08, 0.05), (1.2, 0.0)),
    "trainer-c" -> LocalDataset(18, Vector(0.50, -0.16, 0.12), (5.2, 0.0)),
  )

  private final class NebulaState extends Stateful:
    var globalModel: Model on Coordinator = place(initialModel)
    var roundSummaries: Vector[RoundSummary] on Coordinator = place(Vector.empty)
    var latestTopology: Option[TopologyStatus] on Participant = place(None)
    var lastLocalUpdate: Option[LocalUpdate] on Participant = place(None)

  private def formatMetrics(metrics: LocalMetrics): String =
    f"loss=${metrics.loss}%.3f acc=${metrics.accuracy}%.3f"

  private def formatTopology(status: TopologyStatus): String =
    val avgDistance = f"${status.avgDistance}%.2f"
    val stability = f"${status.stability}%.2f"
    s"${status.nodeId}: close=${status.closeNeighbors}, avgDist=$avgDistance, " +
      s"seedDist=${prettyDistance(status.distanceToSeed)}, stability=$stability, " +
      s"connected=${status.connected}, eligible=${status.eligible}"

  private def formatUpdate(update: LocalUpdate): String =
    s"${update.nodeId}@r${update.round}: samples=${update.samples}, ${formatMetrics(update.metrics)}, model=${pretty(update.model)}"

  private def topologyAwareness(state: NebulaState^)(using Network, Placement, Collective): Unit =
    val localPeer = peerAddress.asInstanceOf[String]
    given DistanceSensor = new DistanceSensor:
      val localPosition = datasets(localPeer).position
      def nbrRange(using Collective, VM): Field[Double] =
        nbr(localPosition)
          .map: remotePosition =>
            val dx = localPosition._1 - remotePosition._1
            val dy = localPosition._2 - remotePosition._2
            math.sqrt((dx * dx) + (dy * dy))

    val topologySignal = Collective[Participant](RoundInterval):
      val rawRanges = nbrRange
      val distances = rawRanges.withoutSelf.values.toList
      val closeNeighbors = distances.count(_ <= CloseNeighborThreshold)
      val avgDistance =
        if distances.isEmpty then 0.0
        else distances.sum / distances.size.toDouble
      val boundedMetric = rawRanges.map: distance =>
        if distance <= CloseNeighborThreshold then distance
        else InfinityDistance
      val distanceFromSeed = G(localPeer == AggregationSeed, 0.0, _ + boundedMetric.minWithoutSelf(InfinityDistance), () => boundedMetric)
      val localDensity =
        if distances.isEmpty then 0.0
        else closeNeighbors.toDouble / distances.size.toDouble
      val stability = rep(localDensity): previous =>
        (previous * 0.65) + (localDensity * 0.35)
      val connectedToSeed = distanceFromSeed < InfinityDistance
      val eligible =
        connectedToSeed &&
          distanceFromSeed <= FederationRadius &&
          stability >= StabilityThreshold
      TopologyStatus(
        nodeId = localPeer,
        closeNeighbors = closeNeighbors,
        avgDistance = avgDistance,
        distanceToSeed = distanceFromSeed,
        stability = stability,
        connected = connectedToSeed,
        eligible = eligible,
      )

    on[Participant]:
      take(topologySignal).subscribe: status =>
        if status.avgDistance > 0.0 || status.closeNeighbors > 0 then
          state.latestTopology = place(Some(status))
          println(s"[${status.nodeId}] topology -> ${formatTopology(status)}")
  end topologyAwareness

  private def trainingProtocol(state: NebulaState^)(using Network, Placement, Choreography): Unit =
    def runRound(round: Int): Unit =
      if round > TotalRounds then ()
      else
        Choreography:
          val roundAtCoordinator = on[Coordinator](round)
          val modelAtCoordinator = on[Coordinator](take(state.globalModel))
          val currentRound = broadcast[Coordinator, Int](roundAtCoordinator)
          val currentModel = broadcast[Coordinator, Model](modelAtCoordinator)

          val participantUpdate = on[Participant]:
            val nodeId = peerAddress.asInstanceOf[String]
            val eligible = take(state.latestTopology).exists(_.eligible)
            if eligible then
              val dataset = datasets(nodeId)
              val (trainedModel, metrics) = trainLocally(currentModel, dataset, currentRound)
              val update = LocalUpdate(nodeId, currentRound, dataset.samples, trainedModel, metrics)
              state.lastLocalUpdate = place(Some(update))
              println(s"[$nodeId] round $currentRound trained -> ${formatUpdate(update)}")
              Some(update)
            else
              state.lastLocalUpdate = place(None)
              val reason = take(state.latestTopology)
                .map(status => s"outside federation region (${formatTopology(status)})")
                .getOrElse("missing collective state")
              println(s"[$nodeId] round $currentRound skipped -> $reason")
              None

          val gatheredUpdates = gather[Participant, Coordinator](participantUpdate)

          on[Coordinator]:
            val collected = take(gatheredUpdates).toList.sortBy(_._1.toString)
            val updates = collected.collect { case (_, Some(update)) => update }
            val dropped = collected.collect { case (nodeId, None) => nodeId.toString }
            val aggregated = fedAvg(updates, take(state.globalModel))
            val summary = RoundSummary(
              round = currentRound,
              contributors = updates.map(_.nodeId).sorted,
              dropped = dropped,
              aggregated = aggregated,
            )
            state.globalModel = place(aggregated)
            state.roundSummaries = place(take(state.roundSummaries) :+ summary)
            println(
              s"[coordinator] round $currentRound aggregated from ${summary.contributors.mkString(", ")}; " +
                s"dropped=${summary.dropped.mkString(", ")} -> ${pretty(summary.aggregated)}",
            )

        runRound(round + 1)

    runRound(1)
  end trainingProtocol

  private def dashboardReport(state: NebulaState^)(using Network, Placement, Multitier): Unit = Multitier:
    val finalModelAtCoordinator = on[Coordinator](take(state.globalModel))
    val summariesAtCoordinator = on[Coordinator](take(state.roundSummaries).toList)
    val topologyAtParticipants = on[Participant](take(state.latestTopology))
    val updateAtParticipants = on[Participant](take(state.lastLocalUpdate))

    on[Dashboard]:
      val finalModel = asLocal[Dashboard, Coordinator](finalModelAtCoordinator)
      val summaries = asLocal[Dashboard, Coordinator](summariesAtCoordinator)
      val topologies = asLocalAll[Dashboard, Participant](topologyAtParticipants).toMap.toList.sortBy(_._1.toString)
      val updates = asLocalAll[Dashboard, Participant](updateAtParticipants).toMap.toList.sortBy(_._1.toString)

      println("\n=== Nebula Dashboard ===")
      println(s"Collective seed: $AggregationSeed, federation radius: $FederationRadius, stability threshold: $StabilityThreshold")
      println(s"Final aggregated model: ${pretty(finalModel)}")
      println("Topology snapshots:")
      topologies.foreach:
        case (_, Some(status)) => println(s" - ${formatTopology(status)}")
        case (nodeId, None) => println(s" - ${nodeId.toString}: missing topology snapshot")
      println("Latest local updates:")
      updates.foreach:
        case (_, Some(update)) => println(s" - ${formatUpdate(update)}")
        case (nodeId, None) => println(s" - ${nodeId.toString}: no contributed update")
      println("Round summaries:")
      summaries.foreach: summary =>
        println(
          s" - round ${summary.round}: contributors=${summary.contributors.mkString(", ")}, " +
            s"dropped=${summary.dropped.mkString(", ")}, model=${pretty(summary.aggregated)}",
        )

  private def nebulaApp(using Network, Placement, Choreography, Collective, Multitier): Unit =
    val state = NebulaState()
    topologyAwareness(state)
    Thread.sleep(TopologyWarmupMillis)
    trainingProtocol(state)
    dashboardReport(state)

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
    println("Running Nebula LociX port...")

    val broker = InMemoryNetwork.broker()
    val coordinatorNetwork = InMemoryNetwork[Coordinator]("coordinator", broker)
    val dashboardNetwork = InMemoryNetwork[Dashboard]("dashboard", broker)
    val trainerANetwork = InMemoryNetwork[Participant]("trainer-a", broker)
    val trainerBNetwork = InMemoryNetwork[Participant]("trainer-b", broker)
    val trainerCNetwork = InMemoryNetwork[Participant]("trainer-c", broker)

    val futures = Seq(
      Future(handleProgramForPeer[Coordinator](coordinatorNetwork)(nebulaApp)),
      Future(handleProgramForPeer[Dashboard](dashboardNetwork)(nebulaApp)),
      Future(handleProgramForPeer[Participant](trainerANetwork)(nebulaApp)),
      Future(handleProgramForPeer[Participant](trainerBNetwork)(nebulaApp)),
      Future(handleProgramForPeer[Participant](trainerCNetwork)(nebulaApp)),
    )

    Await.result(Future.sequence(futures), Duration.Inf)
end Nebula
