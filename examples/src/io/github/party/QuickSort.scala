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

object QuickSort:
  type A <: { type Tie <: Single[B] & Single[C] }
  type B <: { type Tie <: Single[A] & Single[C] }
  type C <: { type Tie <: Single[A] & Single[B] }

  final case class Partition(lower: List[Int], pivot: Int, greater: List[Int])

  private def partition(list: List[Int]): Partition =
    val pivotIndex = list.length / 2
    val pivot = list(pivotIndex)
    val rest = list.patch(pivotIndex, Nil, 1)
    val (lower, greater) = rest.partition(_ <= pivot)
    Partition(lower, pivot, greater)

  private def sortAtA(listAtA: List[Int] on A)(using Network, Placement, Choreography): List[Int] on A = Choreography:
    val doneAtA: Boolean on A = on[A]:
      take(listAtA).size <= 1
    if broadcast[A, Boolean](doneAtA) then listAtA
    else
      val partitionAtA: Partition on A = on[A]:
        partition(take(listAtA))
      val greaterAtB = comm[A, B](on[A] { take(partitionAtA).greater })
      val lowerAtC = comm[A, C](on[A] { take(partitionAtA).lower })
      val sortedGreaterAtB = sortAtB(greaterAtB)
      val sortedLowerAtC = sortAtC(lowerAtC)
      val sortedGreaterAtA = comm[B, A](sortedGreaterAtB)
      val sortedLowerAtA = comm[C, A](sortedLowerAtC)
      on[A]:
        take(sortedLowerAtA) ++ (take(partitionAtA).pivot :: take(sortedGreaterAtA))

  private def sortAtB(listAtB: List[Int] on B)(using Network, Placement, Choreography): List[Int] on B = Choreography:
    val doneAtB: Boolean on B = on[B]:
      take(listAtB).size <= 1
    if broadcast[B, Boolean](doneAtB) then listAtB
    else
      val partitionAtB: Partition on B = on[B]:
        partition(take(listAtB))
      val greaterAtC = comm[B, C](on[B] { take(partitionAtB).greater })
      val lowerAtA = comm[B, A](on[B] { take(partitionAtB).lower })
      val sortedGreaterAtC = sortAtC(greaterAtC)
      val sortedLowerAtA = sortAtA(lowerAtA)
      val sortedGreaterAtB = comm[C, B](sortedGreaterAtC)
      val sortedLowerAtB = comm[A, B](sortedLowerAtA)
      on[B]:
        take(sortedLowerAtB) ++ (take(partitionAtB).pivot :: take(sortedGreaterAtB))

  private def sortAtC(listAtC: List[Int] on C)(using Network, Placement, Choreography): List[Int] on C = Choreography:
    val doneAtC: Boolean on C = on[C]:
      take(listAtC).size <= 1
    if broadcast[C, Boolean](doneAtC) then listAtC
    else
      val partitionAtC: Partition on C = on[C]:
        partition(take(listAtC))
      val greaterAtA = comm[C, A](on[C] { take(partitionAtC).greater })
      val lowerAtB = comm[C, B](on[C] { take(partitionAtC).lower })
      val sortedGreaterAtA = sortAtA(greaterAtA)
      val sortedLowerAtB = sortAtB(lowerAtB)
      val sortedGreaterAtC = comm[A, C](sortedGreaterAtA)
      val sortedLowerAtC = comm[B, C](sortedLowerAtB)
      on[C]:
        take(sortedLowerAtC) ++ (take(partitionAtC).pivot :: take(sortedGreaterAtC))

  def quickSortProtocol(using Network, Placement, Choreography) = Choreography:
    val inputAtA = on[A]:
      List(5, 7, 12, 22, 1, 2, 45)
    val resultAtA = sortAtA(inputAtA)
    on[A]:
      println(s"[A] sorted=${take(resultAtA)}")

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
    val a = InMemoryNetwork[A]("a", broker)
    val b = InMemoryNetwork[B]("b", broker)
    val c = InMemoryNetwork[C]("c", broker)

    val fa = Future { handleProgramForPeer[A](a)(quickSortProtocol) }
    val fb = Future { handleProgramForPeer[B](b)(quickSortProtocol) }
    val fc = Future { handleProgramForPeer[C](c)(quickSortProtocol) }

    Await.result(Future.sequence(Seq(fa, fb, fc)), duration.Duration.Inf)
end QuickSort
