package io.github.party

import java.util.concurrent.CountDownLatch

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ MultitierHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object ChirperInc:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class Chirp(name: String, msg: String)
  final case class FilteredState(kw: String, chirps: List[Chirp])
  final case class ChirpsDelta(to: String, chirps: List[Chirp], keyword: Option[String])

  sealed trait ScriptStep
  final case class Search(keyword: String) extends ScriptStep
  final case class Post(message: String) extends ScriptStep

  sealed trait ClientCommand
  final case class SearchUpdated(sessionId: String, keyword: String) extends ClientCommand
  final case class ChirpSubmitted(chirp: Chirp) extends ClientCommand

  private val scripts: Map[String, List[ScriptStep]] = Map(
    "alice" -> List(Search(""), Post("hello from alice"), Search("scala"), Post("scala is great")),
    "bob" -> List(Search("alice"), Post("bob joined"), Search("great"), Post("this is great")),
    "carol" -> List(Search(""), Post("carol online"), Search("bob"), Post("bob seems happy")),
  )

  private val startupDelayMs = 800L
  private val betweenStepsDelayMs = 180L

  private def searchSuccess(kw: String)(chirp: Chirp): Boolean =
    kw.isEmpty || chirp.msg.toLowerCase.contains(kw.toLowerCase)

  private def applyDelta(state: FilteredState, delta: ChirpsDelta): FilteredState =
    delta.keyword match
      case None => state.copy(chirps = state.chirps ++ delta.chirps)
      case Some(keyword) => FilteredState(keyword, delta.chirps)

  private def renderState(state: FilteredState): String =
    if state.chirps.isEmpty then "<empty>"
    else state.chirps.map(chirp => s"${chirp.name}: ${chirp.msg}").mkString(" | ")

  def chirperIncProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val commandsOnClient = on[Client]:
      val sessionId = peerAddress.asInstanceOf[String]
      val displayName = sessionId.capitalize
      val script = scripts.getOrElse(sessionId, List(Search(""), Post(s"hello from $sessionId")))
      signalBuilder[ClientCommand]: emitter =>
        Thread.sleep(startupDelayMs)
        script.foreach:
          case Search(keyword) =>
            println(s"[Client $sessionId] search '$keyword'")
            emitter.emit(SearchUpdated(sessionId, keyword))
            Thread.sleep(betweenStepsDelayMs)
          case Post(message) =>
            println(s"[Client $sessionId] submit '$message'")
            emitter.emit(ChirpSubmitted(Chirp(displayName, message)))
            Thread.sleep(betweenStepsDelayMs)

    val deltasOnServer = on[Server]:
      val commandStreams = asLocalAll[Server, Client](commandsOnClient).toMap
      println(s"[Server] Tracking ${commandStreams.size} command stream(s).")

      val mergedCommands = Signal.merge(commandStreams.values.toSeq)
      val (deltaSignal, deltaEmitter) = Signal.make[ChirpsDelta]
      val allStreamsClosed = CountDownLatch(commandStreams.size)
      val knownClients = commandStreams.keys.map(_.toString).toList.sorted
      val lock = new AnyRef
      var allChirps = List.empty[Chirp]
      val searchTerms = scala.collection.mutable.Map.from(knownClients.map(_ -> ""))

      commandStreams.toSeq
        .sortBy(_._1.toString)
        .foreach:
          case (clientId, stream) =>
            stream.onClose: () =>
              allStreamsClosed.countDown()
              println(s"[Server] Stream closed for $clientId (${allStreamsClosed.getCount} remaining).")

      mergedCommands.subscribe:
        case SearchUpdated(sessionId, keyword) =>
          val delta = lock.synchronized:
            searchTerms.update(sessionId, keyword)
            val filtered = allChirps.filter(searchSuccess(keyword))
            ChirpsDelta(sessionId, filtered, Some(keyword))
          println(s"[Server] Search reset for $sessionId -> '$keyword'")
          deltaEmitter.emit(delta)
        case ChirpSubmitted(chirp) =>
          val deltas = lock.synchronized:
            allChirps = allChirps :+ chirp
            knownClients.flatMap: clientId =>
              val keyword = searchTerms.getOrElse(clientId, "")
              if searchSuccess(keyword)(chirp) then Some(ChirpsDelta(clientId, List(chirp), None))
              else None
          println(s"[Server] New chirp '${chirp.name}: ${chirp.msg}'")
          deltas.foreach(deltaEmitter.emit)

      Future:
        allStreamsClosed.await()
        println("[Server] All command streams closed. Closing incremental delta stream.")
        deltaEmitter.close()

      deltaSignal

    on[Client]:
      val sessionId = peerAddress.asInstanceOf[String]
      val incomingDeltas = asLocal[Client, Server](deltasOnServer)
      val done = CountDownLatch(1)
      val lock = new AnyRef
      var state = FilteredState("", List.empty)

      incomingDeltas.subscribe: delta =>
        if delta.to == sessionId then
          val snapshot = lock.synchronized:
            state = applyDelta(state, delta)
            state
          println(s"[Client $sessionId] view kw='${snapshot.kw}' -> ${renderState(snapshot)}")

      incomingDeltas.onClose: () =>
        println(s"[Client $sessionId] incremental stream closed.")
        done.countDown()

      done.await()
  end chirperIncProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running ChirperInc scripted port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val aliceNetwork = InMemoryNetwork[Client]("alice", broker)
    val bobNetwork = InMemoryNetwork[Client]("bob", broker)
    val carolNetwork = InMemoryNetwork[Client]("carol", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(chirperIncProtocol) }
    val aliceFuture = Future { handleProgramForPeer[Client](aliceNetwork)(chirperIncProtocol) }
    val bobFuture = Future { handleProgramForPeer[Client](bobNetwork)(chirperIncProtocol) }
    val carolFuture = Future { handleProgramForPeer[Client](carolNetwork)(chirperIncProtocol) }

    val all = Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("ChirperInc done.")
end ChirperInc
