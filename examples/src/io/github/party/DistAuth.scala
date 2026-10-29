package io.github.party

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

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

object DistAuth:
  type Client <: { type Tie <: Single[IP] }
  type Service <: { type Tie <: Single[IP] }
  type IP <: { type Tie <: Single[Client] & Single[Service] }

  final case class Credentials(username: String, password: String)
  final case class AuthToken(id: String)
  final case class Profile(salt: String, hash: String)

  private def hashPassword(salt: String, password: String): String =
    val digest = MessageDigest.getInstance("SHA3-256")
    val bytes = digest.digest((salt + password).getBytes(StandardCharsets.UTF_8))
    Base64.getEncoder.encodeToString(bytes)

  private val registry = Map(
    "john" -> Profile("salt-john", hashPassword("salt-john", "doe")),
  )

  private def authenticate(credentialsAtClient: Credentials on Client)(using Network, Placement, Choreography): Unit = Choreography:
    val usernameAtIP = comm[Client, IP](on[Client] { take(credentialsAtClient).username })
    val saltAtIP: String on IP = on[IP]:
      registry.get(take(usernameAtIP)).map(_.salt).getOrElse("missing")
    val saltAtClient = comm[IP, Client](saltAtIP)

    val passwordHashAtClient: String on Client = on[Client]:
      hashPassword(take(saltAtClient), take(credentialsAtClient).password)
    val passwordHashAtIP = comm[Client, IP](passwordHashAtClient)

    val validAtIP: Boolean on IP = on[IP]:
      registry.get(take(usernameAtIP)).exists(_.hash == take(passwordHashAtIP))
    val valid = broadcast[IP, Boolean](validAtIP)

    if valid then
      val tokenAtIP: AuthToken on IP = on[IP]:
        AuthToken(s"token-${take(usernameAtIP)}")
      val tokenAtClient = comm[IP, Client](tokenAtIP)
      val tokenAtService = comm[IP, Service](tokenAtIP)

      on[Client]:
        println(s"[Client] authenticated as ${take(credentialsAtClient).username} with ${take(tokenAtClient).id}")
      on[Service]:
        println(s"[Service] accepted token ${take(tokenAtService).id}")
      on[IP]:
        println(s"[IP] granted access to ${take(usernameAtIP)}")
    else
      on[Client]:
        println(s"[Client] authentication failed for ${take(credentialsAtClient).username}")
      on[Service]:
        println("[Service] access denied")
      on[IP]:
        println(s"[IP] rejected ${take(usernameAtIP)}")

  def distAuthProtocol(using Network, Placement, Choreography) = Choreography:
    List(
      Credentials("john", "doe"),
      Credentials("john", "bad"),
    ).foreach: attempt =>
      val credentialsAtClient = on[Client]:
        println(s"[Client] attempting login for ${attempt.username}")
        attempt
      authenticate(credentialsAtClient)

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
    val client = InMemoryNetwork[Client]("client", broker)
    val service = InMemoryNetwork[Service]("service", broker)
    val ip = InMemoryNetwork[IP]("ip", broker)

    val fc = Future { handleProgramForPeer[Client](client)(distAuthProtocol) }
    val fs = Future { handleProgramForPeer[Service](service)(distAuthProtocol) }
    val fi = Future { handleProgramForPeer[IP](ip)(distAuthProtocol) }

    Await.result(Future.sequence(Seq(fc, fs, fi)), duration.Duration.Inf)
end DistAuth
