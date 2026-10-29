package io.github.party

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ ChoreographyHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object HelloRoles:
  type A <: { type Tie <: Single[B] }
  type B <: { type Tie <: Single[A] }

  def helloRoles(using Network, Placement, Choreography) = Choreography:
    on[A]:
      println("Hello from A")
    on[B]:
      println("Hello from B")

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

    val fa = Future { handleProgramForPeer[A](a)(helloRoles) }
    val fb = Future { handleProgramForPeer[B](b)(helloRoles) }

    Await.result(Future.sequence(Seq(fa, fb)), duration.Duration.Inf)
end HelloRoles
