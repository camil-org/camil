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

object DiffieHellman:
  type Alice <: { type Tie <: Single[Bob] }
  type Bob <: { type Tie <: Single[Alice] }

  private val generator = BigInt(5)
  private val prime = BigInt(23)
  private val alicePrivate = BigInt(6)
  private val bobPrivate = BigInt(15)

  def diffieHellmanProtocol(using Network, Placement, Choreography) = Choreography:
    val alicePublic: BigInt on Alice = on[Alice]:
      generator.modPow(alicePrivate, prime)
    val bobPublic: BigInt on Bob = on[Bob]:
      generator.modPow(bobPrivate, prime)

    val bobPublicAtAlice = comm[Bob, Alice](bobPublic)
    val alicePublicAtBob = comm[Alice, Bob](alicePublic)

    val aliceShared: BigInt on Alice = on[Alice]:
      take(bobPublicAtAlice).modPow(alicePrivate, prime)
    val bobShared: BigInt on Bob = on[Bob]:
      take(alicePublicAtBob).modPow(bobPrivate, prime)

    on[Alice]:
      println(s"[Alice] public=${take(alicePublic)} shared=${take(aliceShared)}")
    on[Bob]:
      println(s"[Bob] public=${take(bobPublic)} shared=${take(bobShared)}")

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
    val alice = InMemoryNetwork[Alice]("alice", broker)
    val bob = InMemoryNetwork[Bob]("bob", broker)

    val fa = Future { handleProgramForPeer[Alice](alice)(diffieHellmanProtocol) }
    val fb = Future { handleProgramForPeer[Bob](bob)(diffieHellmanProtocol) }

    Await.result(Future.sequence(Seq(fa, fb)), duration.Duration.Inf)
end DiffieHellman
