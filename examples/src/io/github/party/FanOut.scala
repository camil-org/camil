package io.github.party

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ ChoreographyHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

/**
 * Fan-Out choreography.
 *
 * Ported from the ChoRus (Rust) fanout example.
 *
 * Alice sends a personalized greeting to each of Bob and Carol. Each recipient prints the message they received.
 */
object FanOut:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // ──────────────────────────────────────────────────────────────────────────

  type Alice <: { type Tie <: Single[Bob] & Single[Carol] }
  type Bob <: { type Tie <: Single[Alice] }
  type Carol <: { type Tie <: Single[Alice] }

  // ──────────────────────────────────────────────────────────────────────────
  // Choreography
  // ──────────────────────────────────────────────────────────────────────────

  def fanOutProtocol(using Network, Placement, Choreography) = Choreography:

    // Alice creates a personalised message for Bob
    val msgForBob: String on Alice = on[Alice]:
      val msg = "Alice says hi to Bob"
      println(s"[Alice] Sending to Bob: $msg")
      msg

    // Alice sends the message to Bob
    val msgAtBob = comm[Alice, Bob](msgForBob)

    // Bob prints the received message
    on[Bob]:
      val received = take(msgAtBob)
      println(s"[Bob] Received: \"$received\"")

    // Alice creates a personalised message for Carol
    val msgForCarol: String on Alice = on[Alice]:
      val msg = "Alice says hi to Carol"
      println(s"[Alice] Sending to Carol: $msg")
      msg

    // Alice sends the message to Carol
    val msgAtCarol = comm[Alice, Carol](msgForCarol)

    // Carol prints the received message
    on[Carol]:
      val received = take(msgAtCarol)
      println(s"[Carol] Received: \"$received\"")

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
    println("Running FanOut choreography...")
    val broker = InMemoryNetwork.broker()
    val aliceNet = InMemoryNetwork[Alice]("alice", broker)
    val bobNet = InMemoryNetwork[Bob]("bob", broker)
    val carolNet = InMemoryNetwork[Carol]("carol", broker)

    val f1 = Future { handleProgramForPeer[Alice](aliceNet)(fanOutProtocol) }
    val f2 = Future { handleProgramForPeer[Bob](bobNet)(fanOutProtocol) }
    val f3 = Future { handleProgramForPeer[Carol](carolNet)(fanOutProtocol) }

    Await.result(
      Future.sequence(Seq(f1, f2, f3)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("FanOut done.")
end FanOut
