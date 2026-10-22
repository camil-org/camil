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
 * Distributed Merge-Sort choreography.
 *
 * Ported from the ChoRus (Rust) mergesort example.
 *
 * Three peers (Alice, Bob, Carol) cooperatively sort a list using merge-sort. Alice holds the initial unsorted list. The list is recursively split
 * and distributed to the three peers in a round-robin fashion:
 *
 * Sort@Alice → prefix to Bob (Sort@Bob), suffix to Carol (Sort@Carol) Sort@Bob → prefix to Carol(Sort@Carol), suffix to Alice (Sort@Alice) Sort@Carol
 * → prefix to Alice(Sort@Alice), suffix to Bob (Sort@Bob)
 *
 * After sorting the halves a distributed merge combines them back, always producing the result at the "primary" peer for that rotation.
 *
 * Because the Scala framework uses structural peer types (not generic type parameters like Rust's `Sort<A, B, C>`), each rotation is written as an
 * explicit function. The communication topology and control-flow are identical to the original Rust implementation.
 */
object MergeSort:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions — fully connected (all-to-all single ties)
  // ──────────────────────────────────────────────────────────────────────────

  type Alice <: { type Tie <: Single[Bob] & Single[Carol] }
  type Bob <: { type Tie <: Single[Alice] & Single[Carol] }
  type Carol <: { type Tie <: Single[Alice] & Single[Bob] }

  // ════════════════════════════════════════════════════════════════════════════
  // Sort — three rotations (one per "primary" peer)
  //
  //   Sort<A,B,C>  ↔  sortAt{A}
  //     prefix → B,  sorted via sortAt{B}   (= Sort<B,C,A>)
  //     suffix → C,  sorted via sortAt{C}   (= Sort<C,A,B>)
  //     merge{B,C} →  result at A
  // ════════════════════════════════════════════════════════════════════════════

  /**
   * Sort a list located at Alice, using Bob and Carol as helpers. Result at Alice. Corresponds to `Sort<Alice, Bob, Carol>` in the Rust
   * implementation.
   */
  def sortAtAlice(listAtA: List[Long] on Alice)(using Network, Placement, Choreography): List[Long] on Alice = Choreography:
    val isSmall: Boolean on Alice = on[Alice]:
      take(listAtA).length <= 1
    val isSmallAll = broadcast[Alice, Boolean](isSmall)
    if isSmallAll then listAtA
    else
      val pivot: Int on Alice = on[Alice]:
        take(listAtA).length / 2
      val prefixAtA: List[Long] on Alice = on[Alice]:
        take(listAtA).take(take(pivot))
      val prefixAtB = comm[Alice, Bob](prefixAtA)
      val sortedPrefixAtB = sortAtBob(prefixAtB)
      val suffixAtA: List[Long] on Alice = on[Alice]:
        take(listAtA).drop(take(pivot))
      val suffixAtC = comm[Alice, Carol](suffixAtA)
      val sortedSuffixAtC = sortAtCarol(suffixAtC)
      mergeBC(sortedPrefixAtB, sortedSuffixAtC)

  /**
   * Sort a list located at Bob, using Carol and Alice as helpers. Result at Bob. Corresponds to `Sort<Bob, Carol, Alice>` in the Rust implementation.
   */
  def sortAtBob(listAtB: List[Long] on Bob)(using Network, Placement, Choreography): List[Long] on Bob = Choreography:
    val isSmall: Boolean on Bob = on[Bob]:
      take(listAtB).length <= 1
    val isSmallAll = broadcast[Bob, Boolean](isSmall)
    if isSmallAll then listAtB
    else
      val pivot: Int on Bob = on[Bob]:
        take(listAtB).length / 2
      val prefixAtB: List[Long] on Bob = on[Bob]:
        take(listAtB).take(take(pivot))
      val prefixAtC = comm[Bob, Carol](prefixAtB)
      val sortedPrefixAtC = sortAtCarol(prefixAtC)
      val suffixAtB: List[Long] on Bob = on[Bob]:
        take(listAtB).drop(take(pivot))
      val suffixAtA = comm[Bob, Alice](suffixAtB)
      val sortedSuffixAtA = sortAtAlice(suffixAtA)
      mergeCA(sortedPrefixAtC, sortedSuffixAtA)

  /**
   * Sort a list located at Carol, using Alice and Bob as helpers. Result at Carol. Corresponds to `Sort<Carol, Alice, Bob>` in the Rust
   * implementation.
   */
  def sortAtCarol(listAtC: List[Long] on Carol)(using Network, Placement, Choreography): List[Long] on Carol = Choreography:
    val isSmall: Boolean on Carol = on[Carol]:
      take(listAtC).length <= 1
    val isSmallAll = broadcast[Carol, Boolean](isSmall)
    if isSmallAll then listAtC
    else
      val pivot: Int on Carol = on[Carol]:
        take(listAtC).length / 2
      val prefixAtC: List[Long] on Carol = on[Carol]:
        take(listAtC).take(take(pivot))
      val prefixAtA = comm[Carol, Alice](prefixAtC)
      val sortedPrefixAtA = sortAtAlice(prefixAtA)
      val suffixAtC: List[Long] on Carol = on[Carol]:
        take(listAtC).drop(take(pivot))
      val suffixAtB = comm[Carol, Bob](suffixAtC)
      val sortedSuffixAtB = sortAtBob(suffixAtB)
      mergeAB(sortedPrefixAtA, sortedSuffixAtB)

  // ════════════════════════════════════════════════════════════════════════════
  // Merge — three rotations
  //
  //   Merge<B,C> produces result at A (the implicit third location):
  //     mergeBC  → result at Alice    (Merge<Bob,   Carol>)
  //     mergeCA  → result at Bob      (Merge<Carol, Alice>)
  //     mergeAB  → result at Carol    (Merge<Alice, Bob>)
  // ════════════════════════════════════════════════════════════════════════════

  /**
   * Merge sorted prefix@Bob and suffix@Carol → result@Alice. Corresponds to `Merge<Bob, Carol>` (result at Alice) in Rust.
   */
  def mergeBC(prefixAtB: List[Long] on Bob, suffixAtC: List[Long] on Carol)(using Network, Placement, Choreography): List[Long] on Alice =
    Choreography:
      val isPrefixEmpty: Boolean on Bob = on[Bob]:
        take(prefixAtB).isEmpty
      val isPrefixEmptyAll = broadcast[Bob, Boolean](isPrefixEmpty)
      if isPrefixEmptyAll then comm[Carol, Alice](suffixAtC)
      else
        val isSuffixEmpty: Boolean on Carol = on[Carol]:
          take(suffixAtC).isEmpty
        val isSuffixEmptyAll = broadcast[Carol, Boolean](isSuffixEmpty)
        if isSuffixEmptyAll then comm[Bob, Alice](prefixAtB)
        else
          val headPrefixAtB: Long on Bob = on[Bob]:
            take(prefixAtB).head
          val headPrefixAtC = comm[Bob, Carol](headPrefixAtB)
          val headSuffixAtC: Long on Carol = on[Carol]:
            take(suffixAtC).head
          val cmp: Boolean on Carol = on[Carol]:
            take(headPrefixAtC) <= take(headSuffixAtC)
          val cmpAll = broadcast[Carol, Boolean](cmp)
          if cmpAll then
            val headAtA = comm[Bob, Alice](headPrefixAtB)
            val restPrefixAtB: List[Long] on Bob = on[Bob]:
              take(prefixAtB).drop(1)
            val partialAtA = mergeBC(restPrefixAtB, suffixAtC)
            on[Alice]:
              take(headAtA) :: take(partialAtA)
          else
            val headAtA = comm[Carol, Alice](headSuffixAtC)
            val restSuffixAtC: List[Long] on Carol = on[Carol]:
              take(suffixAtC).drop(1)
            val partialAtA = mergeBC(prefixAtB, restSuffixAtC)
            on[Alice]:
              take(headAtA) :: take(partialAtA)
        end if
      end if

  /**
   * Merge sorted prefix@Carol and suffix@Alice → result@Bob. Corresponds to `Merge<Carol, Alice>` (result at Bob) in Rust.
   */
  def mergeCA(prefixAtC: List[Long] on Carol, suffixAtA: List[Long] on Alice)(using Network, Placement, Choreography): List[Long] on Bob =
    Choreography:
      val isPrefixEmpty: Boolean on Carol = on[Carol]:
        take(prefixAtC).isEmpty
      val isPrefixEmptyAll = broadcast[Carol, Boolean](isPrefixEmpty)
      if isPrefixEmptyAll then comm[Alice, Bob](suffixAtA)
      else
        val isSuffixEmpty: Boolean on Alice = on[Alice]:
          take(suffixAtA).isEmpty
        val isSuffixEmptyAll = broadcast[Alice, Boolean](isSuffixEmpty)
        if isSuffixEmptyAll then comm[Carol, Bob](prefixAtC)
        else
          val headPrefixAtC: Long on Carol = on[Carol]:
            take(prefixAtC).head
          val headPrefixAtA = comm[Carol, Alice](headPrefixAtC)
          val headSuffixAtA: Long on Alice = on[Alice]:
            take(suffixAtA).head
          val cmp: Boolean on Alice = on[Alice]:
            take(headPrefixAtA) <= take(headSuffixAtA)
          val cmpAll = broadcast[Alice, Boolean](cmp)
          if cmpAll then
            val headAtB = comm[Carol, Bob](headPrefixAtC)
            val restPrefixAtC: List[Long] on Carol = on[Carol]:
              take(prefixAtC).drop(1)
            val partialAtB = mergeCA(restPrefixAtC, suffixAtA)
            on[Bob]:
              take(headAtB) :: take(partialAtB)
          else
            val headAtB = comm[Alice, Bob](headSuffixAtA)
            val restSuffixAtA: List[Long] on Alice = on[Alice]:
              take(suffixAtA).drop(1)
            val partialAtB = mergeCA(prefixAtC, restSuffixAtA)
            on[Bob]:
              take(headAtB) :: take(partialAtB)
        end if
      end if

  /**
   * Merge sorted prefix@Alice and suffix@Bob → result@Carol. Corresponds to `Merge<Alice, Bob>` (result at Carol) in Rust.
   */
  def mergeAB(prefixAtA: List[Long] on Alice, suffixAtB: List[Long] on Bob)(using Network, Placement, Choreography): List[Long] on Carol =
    Choreography:
      val isPrefixEmpty: Boolean on Alice = on[Alice]:
        take(prefixAtA).isEmpty
      val isPrefixEmptyAll = broadcast[Alice, Boolean](isPrefixEmpty)
      if isPrefixEmptyAll then comm[Bob, Carol](suffixAtB)
      else
        val isSuffixEmpty: Boolean on Bob = on[Bob]:
          take(suffixAtB).isEmpty
        val isSuffixEmptyAll = broadcast[Bob, Boolean](isSuffixEmpty)
        if isSuffixEmptyAll then comm[Alice, Carol](prefixAtA)
        else
          val headPrefixAtA: Long on Alice = on[Alice]:
            take(prefixAtA).head
          val headPrefixAtB = comm[Alice, Bob](headPrefixAtA)
          val headSuffixAtB: Long on Bob = on[Bob]:
            take(suffixAtB).head
          val cmp: Boolean on Bob = on[Bob]:
            take(headPrefixAtB) <= take(headSuffixAtB)
          val cmpAll = broadcast[Bob, Boolean](cmp)
          if cmpAll then
            val headAtC = comm[Alice, Carol](headPrefixAtA)
            val restPrefixAtA: List[Long] on Alice = on[Alice]:
              take(prefixAtA).drop(1)
            val partialAtC = mergeAB(restPrefixAtA, suffixAtB)
            on[Carol]:
              take(headAtC) :: take(partialAtC)
          else
            val headAtC = comm[Bob, Carol](headSuffixAtB)
            val restSuffixAtB: List[Long] on Bob = on[Bob]:
              take(suffixAtB).drop(1)
            val partialAtC = mergeAB(prefixAtA, restSuffixAtB)
            on[Carol]:
              take(headAtC) :: take(partialAtC)
        end if
      end if

  // ══════════════════════════════════════════════════════════════════════════
  // Main choreography
  // ══════════════════════════════════════════════════════════════════════════

  def mergeSortProtocol(using Network, Placement, Choreography) = Choreography:
    val shuffledList: List[Long] on Alice = on[Alice]:
      val list = List(9L, 1L, 4L, 7L, 5L, 2L, 3L, 0L, 6L, 8L)
      println(s"[Alice] Unsorted list: $list")
      list

    val sortedList = sortAtAlice(shuffledList)

    on[Alice]:
      val sorted = take(sortedList)
      println(s"[Alice] Sorted list: $sorted")

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
    println("Running MergeSort choreography...")
    val broker = InMemoryNetwork.broker()
    val aliceNet = InMemoryNetwork[Alice]("alice", broker)
    val bobNet = InMemoryNetwork[Bob]("bob", broker)
    val carolNet = InMemoryNetwork[Carol]("carol", broker)

    val f1 = Future { handleProgramForPeer[Alice](aliceNet)(mergeSortProtocol) }
    val f2 = Future { handleProgramForPeer[Bob](bobNet)(mergeSortProtocol) }
    val f3 = Future { handleProgramForPeer[Carol](carolNet)(mergeSortProtocol) }

    Await.result(
      Future.sequence(Seq(f1, f2, f3)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("MergeSort done.")
end MergeSort
