package io.github.party

import java.time.LocalDateTime

import scala.concurrent.*

import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.*
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.*
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object EmailApplication:
  type Server <: { type Tie <: Single[Client] }
  type Client <: { type Tie <: Single[Server] }

  case class Email(date: LocalDateTime, content: String)

  def emailApp(using Network, Placement, Multitier) = Multitier:
    val filters = on[Client] { ("Camil", 7) }
    val allEmails = on[Server]:
      Seq(
        Email(LocalDateTime.now().minusDays(1), "Welcome to Camil!"),
        Email(LocalDateTime.now().minusDays(5), "Your Camil subscription is active."),
        Email(LocalDateTime.now().minusDays(10), "Don't miss our Camil webinar."),
        Email(LocalDateTime.now().minusDays(15), "Learn more about Camil."),
      )
    val filteredEmails = on[Server]:
      val (words, days) = asLocal(filters)
      take(allEmails).filter(email => email.content.contains(words) && email.date.isAfter(LocalDateTime.now().minusDays(days)))
    val clientEmails = on[Client]:
      val emails = asLocal(filteredEmails)
      emails.foreach { email =>
        println(s"[$peerAddress] Received email: ${email.content} (Date: ${email.date})")
      }
      println(s"[$peerAddress] Email search completed.")

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Multitier) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    import scala.concurrent.ExecutionContext.Implicits.global

    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server-address", broker)
    val clientNetwork = InMemoryNetwork[Client]("client-address", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(emailApp) }
    val clientFuture = Future { handleProgramForPeer[Client](clientNetwork)(emailApp) }

    val results = Future.sequence(Seq(serverFuture, clientFuture))
    Await.result(results, scala.concurrent.duration.Duration.Inf)
end EmailApplication
