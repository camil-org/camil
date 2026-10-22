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

object Karatsuba:
  type A <: { type Tie <: Single[B] & Single[C] }
  type B <: { type Tie <: Single[A] & Single[C] }
  type C <: { type Tie <: Single[A] & Single[B] }

  final case class Parts(h1: Long, l1: Long, h2: Long, l2: Long, splitter: Long)

  private def split(n1: Long, n2: Long): Parts =
    val digits = math.max(n1.toString.length, n2.toString.length)
    val splitter = math.pow(10, (digits / 2).toDouble).toLong
    Parts(n1 / splitter, n1 % splitter, n2 / splitter, n2 % splitter, splitter)

  private def multiplyAtA(valuesAtA: (Long, Long) on A)(using Network, Placement, Choreography): Long on A = Choreography:
    val doneAtA: Boolean on A = on[A]:
      val (n1, n2) = take(valuesAtA)
      n1 < 10 || n2 < 10
    if broadcast[A, Boolean](doneAtA) then
      on[A]:
        val (n1, n2) = take(valuesAtA)
        n1 * n2
    else
      val partsAtA: Parts on A = on[A]:
        val (n1, n2) = take(valuesAtA)
        split(n1, n2)
      val lowsAtB = comm[A, B](on[A] { val p = take(partsAtA); (p.l1, p.l2) })
      val highsAtC = comm[A, C](on[A] { val p = take(partsAtA); (p.h1, p.h2) })
      val sumsAtA: (Long, Long) on A = on[A]:
        val p = take(partsAtA)
        (p.l1 + p.h1, p.l2 + p.h2)

      val z0AtB = multiplyAtB(lowsAtB)
      val z2AtC = multiplyAtC(highsAtC)
      val crossAtA = multiplyAtA(sumsAtA)
      val z0AtA = comm[B, A](z0AtB)
      val z2AtA = comm[C, A](z2AtC)

      on[A]:
        val p = take(partsAtA)
        val z0 = take(z0AtA)
        val z2 = take(z2AtA)
        val z1 = take(crossAtA) - z2 - z0
        z2 * p.splitter * p.splitter + z1 * p.splitter + z0
    end if

  private def multiplyAtB(valuesAtB: (Long, Long) on B)(using Network, Placement, Choreography): Long on B = Choreography:
    val doneAtB: Boolean on B = on[B]:
      val (n1, n2) = take(valuesAtB)
      n1 < 10 || n2 < 10
    if broadcast[B, Boolean](doneAtB) then
      on[B]:
        val (n1, n2) = take(valuesAtB)
        n1 * n2
    else
      val partsAtB: Parts on B = on[B]:
        val (n1, n2) = take(valuesAtB)
        split(n1, n2)
      val lowsAtC = comm[B, C](on[B] { val p = take(partsAtB); (p.l1, p.l2) })
      val highsAtA = comm[B, A](on[B] { val p = take(partsAtB); (p.h1, p.h2) })
      val sumsAtB: (Long, Long) on B = on[B]:
        val p = take(partsAtB)
        (p.l1 + p.h1, p.l2 + p.h2)

      val z0AtC = multiplyAtC(lowsAtC)
      val z2AtA = multiplyAtA(highsAtA)
      val crossAtB = multiplyAtB(sumsAtB)
      val z0AtB = comm[C, B](z0AtC)
      val z2AtB = comm[A, B](z2AtA)

      on[B]:
        val p = take(partsAtB)
        val z0 = take(z0AtB)
        val z2 = take(z2AtB)
        val z1 = take(crossAtB) - z2 - z0
        z2 * p.splitter * p.splitter + z1 * p.splitter + z0
    end if

  private def multiplyAtC(valuesAtC: (Long, Long) on C)(using Network, Placement, Choreography): Long on C = Choreography:
    val doneAtC: Boolean on C = on[C]:
      val (n1, n2) = take(valuesAtC)
      n1 < 10 || n2 < 10
    if broadcast[C, Boolean](doneAtC) then
      on[C]:
        val (n1, n2) = take(valuesAtC)
        n1 * n2
    else
      val partsAtC: Parts on C = on[C]:
        val (n1, n2) = take(valuesAtC)
        split(n1, n2)
      val lowsAtA = comm[C, A](on[C] { val p = take(partsAtC); (p.l1, p.l2) })
      val highsAtB = comm[C, B](on[C] { val p = take(partsAtC); (p.h1, p.h2) })
      val sumsAtC: (Long, Long) on C = on[C]:
        val p = take(partsAtC)
        (p.l1 + p.h1, p.l2 + p.h2)

      val z0AtA = multiplyAtA(lowsAtA)
      val z2AtB = multiplyAtB(highsAtB)
      val crossAtC = multiplyAtC(sumsAtC)
      val z0AtC = comm[A, C](z0AtA)
      val z2AtC = comm[B, C](z2AtB)

      on[C]:
        val p = take(partsAtC)
        val z0 = take(z0AtC)
        val z2 = take(z2AtC)
        val z1 = take(crossAtC) - z2 - z0
        z2 * p.splitter * p.splitter + z1 * p.splitter + z0
    end if

  def karatsubaProtocol(using Network, Placement, Choreography) = Choreography:
    val inputAtA = on[A]:
      (15012L, 153531L)
    val resultAtA = multiplyAtA(inputAtA)
    on[A]:
      println(s"[A] product=${take(resultAtA)}")

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

    val fa = Future { handleProgramForPeer[A](a)(karatsubaProtocol) }
    val fb = Future { handleProgramForPeer[B](b)(karatsubaProtocol) }
    val fc = Future { handleProgramForPeer[C](c)(karatsubaProtocol) }

    Await.result(Future.sequence(Seq(fa, fb, fc)), duration.Duration.Inf)
end Karatsuba
