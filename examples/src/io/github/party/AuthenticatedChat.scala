package io.github.party

import java.util.concurrent.CountDownLatch

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.{ Choreography, Multitier }
import io.github.party.Choreography.*
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ ChoreographyHandler, MultitierHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object AuthenticatedChat:
  type AuthServer <: { type Tie <: Multiple[Participant] }
  type Participant <: { type Tie <: Single[AuthServer] }

  case class Credentials(username: String, password: String)
  case class ChatMessage(author: String, text: String)

  private val validUsers: Map[String, String] = Map(
    "alice" -> "s3cr3t",
    "bob" -> "hunter2",
    "carol" -> "p4ssw0rd",
  )

  def authenticatedChat(using Network, Placement, Multitier, Choreography): Unit =
    val grantedAccesses = Choreography:
      val credentials: Credentials on Participant = on[Participant]:
        peerAddress.toString match
          case a if a.contains("alice") => Credentials("alice", "s3cr3t")
          case a if a.contains("bob") => Credentials("bob", "hunter2")
          case a if a.contains("carol") => Credentials("carol", "p4ssw0rd")
          case _ => Credentials("unknown", "wrong") // Invalid credentials for testing

      val allCreds = gather[Participant, AuthServer](credentials)

      val serverDecision = on[AuthServer]:
        val creds = take(allCreds)
        println(s"[AuthServer] Received ${creds.size} credential(s). Validating…")
        creds.map: (addr, cred) =>
          val ok = validUsers.get(cred.username).contains(cred.password)
          if ok then println(s"[AuthServer] ✓  ${cred.username} (@$addr) authenticated")
          else println(s"[AuthServer] ✗  ${cred.username} (@$addr) rejected")
          addr -> ok

      multicast[AuthServer, Participant](serverDecision)

    Multitier:
      val outgoing: Signal[ChatMessage] on Participant = on[Participant]:
        if take(grantedAccesses).getOrElse(peerAddress, false) then
          val name = peerAddress.toString
          signalBuilder[ChatMessage]: emitter =>
            Thread.sleep(100)
            emitter.emit(ChatMessage(name, s"Hi everyone, $name here!"))
            Thread.sleep(150)
            emitter.emit(ChatMessage(name, s"Another message from $name."))
            Thread.sleep(100)
        else Signal.empty

      on[AuthServer]:
        val streams = asLocalAll(outgoing)
        val latch = CountDownLatch(streams.values.size)
        streams.values.foreach: stream =>
          stream.onClose(() => latch.countDown())
          stream.subscribe(msg => println(s"[Chat] ${msg.author}: ${msg.text}"))
        println(s"[AuthServer] Chat started with ${streams.values.size} participant(s).")
        latch.await()
        println("[AuthServer] All participants disconnected. Chat session ended.")
  end authenticatedChat

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("=== Authenticated Chat ===")

    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[AuthServer]("auth-server", broker)
    val aliceNetwork = InMemoryNetwork[Participant]("alice", broker)
    val bobNetwork = InMemoryNetwork[Participant]("bob", broker)
    val carolNetwork = InMemoryNetwork[Participant]("carol", broker)
    val amandNetwork = InMemoryNetwork[Participant]("amand", broker) // Invalid credentials

    val serverFuture = Future { handleProgramForPeer[AuthServer](serverNetwork)(authenticatedChat) }
    val aliceFuture = Future { handleProgramForPeer[Participant](aliceNetwork)(authenticatedChat) }
    val bobFuture = Future { handleProgramForPeer[Participant](bobNetwork)(authenticatedChat) }
    val carolFuture = Future { handleProgramForPeer[Participant](carolNetwork)(authenticatedChat) }
    val amandFuture = Future { handleProgramForPeer[Participant](amandNetwork)(authenticatedChat) }

    Await.result(
      Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture, amandFuture)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("=== Chat session complete ===")
  end main
end AuthenticatedChat
