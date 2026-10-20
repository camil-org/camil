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
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

/**
 * GMW (Goldreich–Micali–Wigderson) Secure Two-Party Computation.
 *
 * Two parties jointly evaluate a boolean circuit without revealing their private inputs. The protocol relies on:
 *
 *   - Additive XOR secret-sharing: each value is split so that s1 XOR s2 = secret.
 *   - Local XOR gates: each party independently XORs its shares.
 *   - AND gates via 1-of-2 Oblivious Transfer (OT): the sender places a two-entry truth-table on the wire; the receiver selects one entry using its
 *     share as the choice bit, without revealing it.
 *   - Reveal: both parties exchange their output shares and reconstruct the final bit.
 *
 * Inputs are mocked (P1 = true, P2 = false). Example circuit: AND(InputP1, InputP2) → true AND false = false.
 */
object GMW:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // ──────────────────────────────────────────────────────────────────────────

  type P1 <: { type Tie <: Single[P2] }
  type P2 <: { type Tie <: Single[P1] }

  // ──────────────────────────────────────────────────────────────────────────
  // Circuit ADT
  // ──────────────────────────────────────────────────────────────────────────

  sealed trait Circuit:
    override def toString: String = this match
      case InputP1 => "InputWire<P1>"
      case InputP2 => "InputWire<P2>"
      case LitWire(b) => s"LitWire($b)"
      case AndGate(l, r) => s"($l) AND ($r)"
      case XorGate(l, r) => s"($l) XOR ($r)"

  /** Secret input wire belonging to P1. */
  case object InputP1 extends Circuit

  /** Secret input wire belonging to P2. */
  case object InputP2 extends Circuit

  /** Publicly-known literal. P1 holds `value`; P2 holds `false`. */
  case class LitWire(value: Boolean) extends Circuit
  case class AndGate(left: Circuit, right: Circuit) extends Circuit
  case class XorGate(left: Circuit, right: Circuit) extends Circuit

  // ──────────────────────────────────────────────────────────────────────────
  // Shares: one Boolean share at each party
  // ──────────────────────────────────────────────────────────────────────────

  final case class Shares(s1: Boolean on P1, s2: Boolean on P2)

  // ──────────────────────────────────────────────────────────────────────────
  // Main choreography
  // ──────────────────────────────────────────────────────────────────────────

  def gmwProtocol(circuit: Circuit)(using Network, Placement, Choreography) = Choreography:

    // ── Secret sharing ──────────────────────────────────────────────────────
    //
    // genShares: generate n-1 random free shares; owner's share is the XOR
    // remainder so that XOR of all shares equals the secret.
    //
    // For 2 parties:
    //   • Owner generates one random bit r (the other party's share).
    //   • Owner's own share = secret XOR r.

    def secretShareByP1(secret: Boolean on P1): Shares =
      val r: Boolean on P1 = on[P1] { scala.util.Random.nextBoolean() }
      val p2Share = comm[P1, P2](r) // send r to P2
      val p1Share: Boolean on P1 = on[P1] { take(secret) ^ take(r) }
      Shares(p1Share, p2Share)

    def secretShareByP2(secret: Boolean on P2): Shares =
      val r: Boolean on P2 = on[P2] { scala.util.Random.nextBoolean() }
      val p1Share = comm[P2, P1](r) // send r to P1
      val p2Share: Boolean on P2 = on[P2] { take(secret) ^ take(r) }
      Shares(p1Share, p2Share)

    // ── XOR gate ────────────────────────────────────────────────────────────
    //
    // Each party locally XORs its own shares — no communication needed.

    def xorGate(u: Shares, v: Shares): Shares =
      Shares(
        on[P1] { take(u.s1) ^ take(v.s1) },
        on[P2] { take(u.s2) ^ take(v.s2) },
      )

    // ── AND gate via OT (fAnd) ───────────────────────────────────────────────
    //
    // For shares (u1, u2) of u and (v1, v2) of v:
    //
    //   r01  = random from P1  (used when P1 is OT sender, P2 is receiver)
    //   r10  = random from P2  (used when P2 is OT sender, P1 is receiver)
    //
    //   OT (P1→P2):
    //     truth table = (m_true = u1 XOR r01,  m_false = r01)
    //     b1 = if v2 then m_true else m_false
    //
    //   OT (P2→P1):
    //     truth table = (m_true = u2 XOR r10,  m_false = r10)
    //     b0 = if v1 then m_true else m_false
    //
    //   c1 = (u1 AND v1) XOR b0 XOR r01
    //   c2 = (u2 AND v2) XOR b1 XOR r10

    def andGate(u: Shares, v: Shares): Shares =
      val r01: Boolean on P1 = on[P1] { scala.util.Random.nextBoolean() }
      val r10: Boolean on P2 = on[P2] { scala.util.Random.nextBoolean() }

      // OT: P1 → P2  (yields b1 at P2)
      val p1Table: (Boolean, Boolean) on P1 = on[P1]:
        (take(u.s1) ^ take(r01), take(r01)) // (m_true, m_false)
      val p1TableAtP2 = comm[P1, P2](p1Table)
      val b1: Boolean on P2 = on[P2]:
        val (mTrue, mFalse) = take(p1TableAtP2)
        if take(v.s2) then mTrue else mFalse

      // OT: P2 → P1  (yields b0 at P1)
      val p2Table: (Boolean, Boolean) on P2 = on[P2]:
        (take(u.s2) ^ take(r10), take(r10)) // (m_true, m_false)
      val p2TableAtP1 = comm[P2, P1](p2Table)
      val b0: Boolean on P1 = on[P1]:
        val (mTrue, mFalse) = take(p2TableAtP1)
        if take(v.s1) then mTrue else mFalse

      Shares(
        on[P1] { (take(u.s1) & take(v.s1)) ^ take(b0) ^ take(r01) },
        on[P2] { (take(u.s2) & take(v.s2)) ^ take(b1) ^ take(r10) },
      )
    end andGate

    // ── Circuit evaluation → secret-shared output ───────────────────────────

    def gmw(c: Circuit): Shares = c match
      case InputP1 =>
        val secret: Boolean on P1 = on[P1] { true } // mocked P1 input
        secretShareByP1(secret)
      case InputP2 =>
        val secret: Boolean on P2 = on[P2] { false } // mocked P2 input
        secretShareByP2(secret)
      case LitWire(b) =>
        Shares(on[P1] { b }, on[P2] { false })
      case AndGate(l, r) =>
        andGate(gmw(l), gmw(r))
      case XorGate(l, r) =>
        xorGate(gmw(l), gmw(r))

    // ── Reveal: exchange shares and reconstruct the final bit ───────────────

    def reveal(shares: Shares): Unit =
      val s1AtP2 = comm[P1, P2](shares.s1)
      val s2AtP1 = comm[P2, P1](shares.s2)
      on[P1]:
        val result = take(shares.s1) ^ take(s2AtP1)
        println(s"[P1] MPC result: $result")
      on[P2]:
        val result = take(s1AtP2) ^ take(shares.s2)
        println(s"[P2] MPC result: $result")

    // ── Protocol entry point ─────────────────────────────────────────────────

    on[P1] { println(s"[P1] Evaluating circuit: $circuit") }
    on[P2] { println(s"[P2] Evaluating circuit: $circuit") }
    reveal(gmw(circuit))

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
    // Circuit: (P1 AND P2) XOR LitWire(true)
    //   P1 input = true, P2 input = false
    //   Expected: (true AND false) XOR true = false XOR true = true
    val circuit: Circuit = XorGate(AndGate(InputP1, InputP2), LitWire(true))
    println(s"Running GMW — circuit: $circuit")
    println("  P1 input (mocked) = true")
    println("  P2 input (mocked) = false")
    println(s"  Expected result   = ${(true & false) ^ true}")

    val broker = InMemoryNetwork.broker()
    val p1Network = InMemoryNetwork[P1]("p1", broker)
    val p2Network = InMemoryNetwork[P2]("p2", broker)

    val p1Future = Future { handleProgramForPeer[P1](p1Network)(gmwProtocol(circuit)) }
    val p2Future = Future { handleProgramForPeer[P2](p2Network)(gmwProtocol(circuit)) }

    Await.result(Future.sequence(Seq(p1Future, p2Future)), scala.concurrent.duration.Duration.Inf)
    println("GMW protocol completed.")
end GMW
