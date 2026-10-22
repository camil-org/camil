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

/**
 * Multicast choreography.
 *
 * Ported from the ChoRus (Rust) multicast example.
 *
 * Alice creates a single message and sends it to both Bob and Carol. Each recipient prints the message they received.
 */
object Multicast:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // ──────────────────────────────────────────────────────────────────────────

  type Alice <: { type Tie <: Single[Bob] & Single[Carol] }
  type Bob <: { type Tie <: Single[Alice] }
  type Carol <: { type Tie <: Single[Alice] }

  // ──────────────────────────────────────────────────────────────────────────
  // Choreography
  // ──────────────────────────────────────────────────────────────────────────

  def multicastProtocol(using Network, Placement, Choreography) = Choreography:

    // Alice creates a message
    val msgAtAlice: String on Alice = on[Alice]:
      println("[Alice] Hello from Alice!")
      "Hello from Alice!"

    // Alice sends the same message to Bob and Carol
    val msgAtBob = comm[Alice, Bob](msgAtAlice)
    val msgAtCarol = comm[Alice, Carol](msgAtAlice)

    // Bob prints the received message
    on[Bob]:
      val msg = take(msgAtBob)
      println(s"[Bob] Received: $msg")

    // Carol prints the received message
    on[Carol]:
      val msg = take(msgAtCarol)
      println(s"[Carol] Received: $msg")

  // ──────────────────────────────────────────────────────────────────────────
  // Peer runner
  // ──────────────────────────────────────────────────────────────────────────

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running Multicast choreography...")
    val broker = InMemoryNetwork.broker()
    val aliceNet = InMemoryNetwork[Alice]("alice", broker)
    val bobNet = InMemoryNetwork[Bob]("bob", broker)
    val carolNet = InMemoryNetwork[Carol]("carol", broker)

    val f1 = Future { handleProgramForPeer[Alice](aliceNet)(multicastProtocol) }
    val f2 = Future { handleProgramForPeer[Bob](bobNet)(multicastProtocol) }
    val f3 = Future { handleProgramForPeer[Carol](carolNet)(multicastProtocol) }

    Await.result(
      Future.sequence(Seq(f1, f2, f3)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("Multicast done.")
end Multicast
