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
 * Federated Lottery — a simplified version of the DPrio lottery protocol.
 *
 * Reference: DPrio – Differentially Private Secure Aggregation
 * https://www.semanticscholar.org/paper/DPrio%3A-Efficient-Differential-Privacy-with-High-for-Keeler-Komlo/ae1b2a4e5beaaa850183ad37e0880bb70ae34f4e
 *
 * Protocol (2 clients, 2 servers, 1 analyst):
 *
 *   1. Each client holds a secret value.
 *   2. Each client splits its secret into two additive shares (one per server): share_s1 = r (random) share_s2 = secret - r and sends share_si to
 *      Server_i.
 *   3. Each server collects one share from each client, ending up with a list [Client1's share, Client2's share].
 *   4. Commitment-based lottery to select a winning client index ω:
 *      a. Each server generates random ρ and salt ψ, computes α = H(ρ ∥ ψ).
 *      b. Servers publish their commitments α to each other.
 *      c. Servers open by sending (ρ, ψ) to each other.
 *      d. Each server verifies the other's commitment.
 *      e. ω = (ρ₁ + ρ₂).abs % numClients (same result at both servers)
 *   5. Each server forwards shares(ω) — the ω-th client's share — to the Analyst.
 *   6. The Analyst sums the two received shares to reconstruct the winner's secret.
 */
object Lottery:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // (two clients, two servers, one analyst — all with explicit typed ties)
  // ──────────────────────────────────────────────────────────────────────────

  type Client1 <: { type Tie <: Single[Server1] & Single[Server2] }
  type Client2 <: { type Tie <: Single[Server1] & Single[Server2] }

  type Server1 <: { type Tie <: Single[Client1] & Single[Client2] & Single[Server2] & Single[Analyst] }
  type Server2 <: { type Tie <: Single[Client1] & Single[Client2] & Single[Server1] & Single[Analyst] }

  type Analyst <: { type Tie <: Single[Server1] & Single[Server2] }

  // ──────────────────────────────────────────────────────────────────────────
  // Cryptographic helper
  // ──────────────────────────────────────────────────────────────────────────

  /** SHA-256 commitment: H(ρ ∥ ψ) returned as a hex string. */
  private def commit(rho: Long, psi: Long): String =
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.update(java.nio.ByteBuffer.allocate(8).putLong(rho).array())
    md.update(java.nio.ByteBuffer.allocate(8).putLong(psi).array())
    md.digest().map("%02x".format(_)).mkString

  // ──────────────────────────────────────────────────────────────────────────
  // Choreography
  // ──────────────────────────────────────────────────────────────────────────

  def lotteryProtocol(using Network, Placement, Choreography) = Choreography:

    // ── Step 1: Each client holds a secret ──────────────────────────────────

    val secret1: Long on Client1 = on[Client1]:
      val s = (scala.util.Random.nextLong() % 1000L).abs
      println(s"[Client1] My secret: $s")
      s

    val secret2: Long on Client2 = on[Client2]:
      val s = (scala.util.Random.nextLong() % 1000L).abs
      println(s"[Client2] My secret: $s")
      s

    // ── Step 2: Each client splits its secret into two additive shares ───────
    //   share_for_s1 = r   (random)
    //   share_for_s2 = secret − r

    val c1shareForS1: Long on Client1 = on[Client1]:
      (scala.util.Random.nextLong() % 1000L).abs

    val c1shareForS2: Long on Client1 = on[Client1]:
      take(secret1) - take(c1shareForS1)

    val c2shareForS1: Long on Client2 = on[Client2]:
      (scala.util.Random.nextLong() % 1000L).abs

    val c2shareForS2: Long on Client2 = on[Client2]:
      take(secret2) - take(c2shareForS1)

    // ── Step 3: Clients send their shares to the respective servers ──────────
    //   Each client → Server1 gets one share, Server2 gets the other.

    val c1s1 = comm[Client1, Server1](c1shareForS1) // Client1 → Server1
    val c1s2 = comm[Client1, Server2](c1shareForS2) // Client1 → Server2
    val c2s1 = comm[Client2, Server1](c2shareForS1) // Client2 → Server1
    val c2s2 = comm[Client2, Server2](c2shareForS2) // Client2 → Server2

    // Each server collects its shares as an ordered list [c1_share, c2_share].
    val s1shares: List[Long] on Server1 = on[Server1]:
      val shares = List(take(c1s1), take(c2s1))
      println(s"[Server1] Received shares: $shares")
      shares

    val s2shares: List[Long] on Server2 = on[Server2]:
      val shares = List(take(c1s2), take(c2s2))
      println(s"[Server2] Received shares: $shares")
      shares

    // ── Step 4a: Each server picks a random ρ and a salt ψ ──────────────────

    val rho1: Long on Server1 = on[Server1]:
      val r = (scala.util.Random.nextLong() % 1000L).abs
      println(s"[Server1] ρ = $r")
      r

    val psi1: Long on Server1 = on[Server1]:
      scala.util.Random.between(1L << 18, 1L << 20)

    val rho2: Long on Server2 = on[Server2]:
      val r = (scala.util.Random.nextLong() % 1000L).abs
      println(s"[Server2] ρ = $r")
      r

    val psi2: Long on Server2 = on[Server2]:
      scala.util.Random.between(1L << 18, 1L << 20)

    // ── Step 4b: Each server computes and publishes its commitment α = H(ρ,ψ)

    val alpha1: String on Server1 = on[Server1]:
      commit(take(rho1), take(psi1))

    val alpha2: String on Server2 = on[Server2]:
      commit(take(rho2), take(psi2))

    // Servers exchange commitments.
    val alpha1AtS2 = comm[Server1, Server2](alpha1)
    val alpha2AtS1 = comm[Server2, Server1](alpha2)

    // ── Step 4c: Servers open their commitments by sending (ρ, ψ) ───────────

    val rho1AtS2 = comm[Server1, Server2](rho1)
    val psi1AtS2 = comm[Server1, Server2](psi1)
    val rho2AtS1 = comm[Server2, Server1](rho2)
    val psi2AtS1 = comm[Server2, Server1](psi2)

    // ── Step 4d: Each server verifies the other's commitment ─────────────────

    on[Server1]:
      val expected = take(alpha2AtS1)
      val recomputed = commit(take(rho2AtS1), take(psi2AtS1))
      require(expected == recomputed, "[Server1] Commitment check FAILED for Server2!")
      println(s"[Server1] Commitment check passed for Server2 ✓")

    on[Server2]:
      val expected = take(alpha1AtS2)
      val recomputed = commit(take(rho1AtS2), take(psi1AtS2))
      require(expected == recomputed, "[Server2] Commitment check FAILED for Server1!")
      println(s"[Server2] Commitment check passed for Server1 ✓")

    // ── Step 4e: Both servers deterministically derive ω = (ρ₁ + ρ₂) % 2 ───
    //   numClients = 2.  Both servers compute the same value from the opened ρ's.

    val omega1: Int on Server1 = on[Server1]:
      val omega = ((take(rho1) + take(rho2AtS1)) % 2).toInt
      println(s"[Server1] Winning index ω = $omega")
      omega

    val omega2: Int on Server2 = on[Server2]:
      val omega = ((take(rho1AtS2) + take(rho2)) % 2).toInt
      println(s"[Server2] Winning index ω = $omega")
      omega

    // ── Step 5: Each server forwards the winner's share to the Analyst ───────

    val chosenShare1: Long on Server1 = on[Server1]:
      val share = take(s1shares)(take(omega1))
      println(s"[Server1] Forwarding share ${share} to Analyst")
      share

    val chosenShare2: Long on Server2 = on[Server2]:
      val share = take(s2shares)(take(omega2))
      println(s"[Server2] Forwarding share ${share} to Analyst")
      share

    val share1AtAnalyst = comm[Server1, Analyst](chosenShare1)
    val share2AtAnalyst = comm[Server2, Analyst](chosenShare2)

    // ── Step 6: Analyst reconstructs the winner's secret from the two shares ─

    on[Analyst]:
      val result = take(share1AtAnalyst) + take(share2AtAnalyst)
      println(s"[Analyst] The winning client's secret is: $result")

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
    println("Running Lottery (DPrio) choreography...")
    val broker = InMemoryNetwork.broker()
    val client1Net = InMemoryNetwork[Client1]("client1", broker)
    val client2Net = InMemoryNetwork[Client2]("client2", broker)
    val server1Net = InMemoryNetwork[Server1]("server1", broker)
    val server2Net = InMemoryNetwork[Server2]("server2", broker)
    val analystNet = InMemoryNetwork[Analyst]("analyst", broker)

    val f1 = Future { handleProgramForPeer[Client1](client1Net)(lotteryProtocol) }
    val f2 = Future { handleProgramForPeer[Client2](client2Net)(lotteryProtocol) }
    val f3 = Future { handleProgramForPeer[Server1](server1Net)(lotteryProtocol) }
    val f4 = Future { handleProgramForPeer[Server2](server2Net)(lotteryProtocol) }
    val f5 = Future { handleProgramForPeer[Analyst](analystNet)(lotteryProtocol) }

    Await.result(
      Future.sequence(Seq(f1, f2, f3, f4, f5)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("Lottery done.")
  end main
end Lottery
