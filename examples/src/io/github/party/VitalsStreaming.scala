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
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object VitalsStreaming:
  type Device <: { type Tie <: Single[Gatherer] }
  type Gatherer <: { type Tie <: Single[Device] }

  final case class Signature(value: String)
  final case class Vitals(id: String, heartRate: String, temperature: String, motion: String)
  final case class VitalsMsg(signature: Signature, content: Vitals)

  private val validSignatures = Set("sig-a", "sig-b")
  private val pseudonyms = Map(
    "alice" -> "patient-1",
    "bob" -> "patient-2",
  )

  private def pseudonymise(vitals: Vitals): Vitals =
    vitals.copy(id = pseudonyms.getOrElse(vitals.id, s"anon-${vitals.id}"))

  private def gather(remaining: List[VitalsMsg] on Device)(using Network, Placement, Choreography): Unit = Choreography:
    val hasNext: Boolean on Device = on[Device]:
      take(remaining).nonEmpty
    if broadcast[Device, Boolean](hasNext) then
      val nextAtDevice: VitalsMsg on Device = on[Device]:
        take(remaining).head
      val tailAtDevice: List[VitalsMsg] on Device = on[Device]:
        take(remaining).iterator.drop(1).toList
      val nextAtGatherer = comm[Device, Gatherer](nextAtDevice)
      on[Gatherer]:
        val msg = take(nextAtGatherer)
        if validSignatures.contains(msg.signature.value) then println(s"[Gatherer] ${pseudonymise(msg.content)}")
      gather(tailAtDevice)
    else
      on[Gatherer]:
        println("[Gatherer] stream complete")

  def vitalsStreamingProtocol(using Network, Placement, Choreography) = Choreography:
    val scriptAtDevice = on[Device]:
      List(
        VitalsMsg(Signature("sig-a"), Vitals("alice", "72", "36.6", "rest")),
        VitalsMsg(Signature("invalid"), Vitals("mallory", "120", "39.5", "run")),
        VitalsMsg(Signature("sig-b"), Vitals("bob", "68", "36.4", "walk")),
      )
    gather(scriptAtDevice)

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
    val device = InMemoryNetwork[Device]("device", broker)
    val gatherer = InMemoryNetwork[Gatherer]("gatherer", broker)

    val fd = Future { handleProgramForPeer[Device](device)(vitalsStreamingProtocol) }
    val fg = Future { handleProgramForPeer[Gatherer](gatherer)(vitalsStreamingProtocol) }

    Await.result(Future.sequence(Seq(fd, fg)), duration.Duration.Inf)
end VitalsStreaming
