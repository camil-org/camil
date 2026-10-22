package io.github.party

import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder

object Chirper:
  type Server <: { type Tie <: Multiple[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class Chirp(name: String, msg: String)
  final case class FilteredChirps(to: String, kw: String, chirps: List[Chirp])

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

  private def renderView(view: FilteredChirps): String =
    if view.chirps.isEmpty then "<empty>"
    else view.chirps.map(chirp => s"${chirp.name}: ${chirp.msg}").mkString(" | ")

  def chirperProtocol(using Network, Placement, Multitier): Unit = Multitier:
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

    val viewsOnServer = on[Server]:
      val commandStreams = asLocalAll[Server, Client](commandsOnClient).toMap
      println(s"[Server] Tracking ${commandStreams.size} command stream(s).")

      val mergedCommands = Signal.merge(commandStreams.values.toSeq)
      val (viewSignal, viewEmitter) = Signal.make[FilteredChirps]
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
          val update = lock.synchronized:
            searchTerms.update(sessionId, keyword)
            val filtered = allChirps.filter(searchSuccess(keyword))
            FilteredChirps(sessionId, keyword, filtered)
          println(s"[Server] Updated search for $sessionId -> '$keyword'")
          viewEmitter.emit(update)
        case ChirpSubmitted(chirp) =>
          val updates = lock.synchronized:
            allChirps = allChirps :+ chirp
            knownClients.map: clientId =>
              val keyword = searchTerms.getOrElse(clientId, "")
              FilteredChirps(clientId, keyword, allChirps.filter(searchSuccess(keyword)))
          println(s"[Server] New chirp '${chirp.name}: ${chirp.msg}'")
          updates.foreach(viewEmitter.emit)

      Future:
        allStreamsClosed.await()
        println("[Server] All command streams closed. Closing filtered views stream.")
        viewEmitter.close()

      viewSignal

    on[Client]:
      val sessionId = peerAddress.asInstanceOf[String]
      val incomingViews = asLocal[Client, Server](viewsOnServer)
      val done = CountDownLatch(1)

      incomingViews.subscribe: view =>
        if view.to == sessionId then println(s"[Client $sessionId] view kw='${view.kw}' -> ${renderView(view)}")

      incomingViews.onClose: () =>
        println(s"[Client $sessionId] filtered views stream closed.")
        done.countDown()

      done.await()
  end chirperProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running Chirper scripted port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val aliceNetwork = InMemoryNetwork[Client]("alice", broker)
    val bobNetwork = InMemoryNetwork[Client]("bob", broker)
    val carolNetwork = InMemoryNetwork[Client]("carol", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(chirperProtocol) }
    val aliceFuture = Future { handleProgramForPeer[Client](aliceNetwork)(chirperProtocol) }
    val bobFuture = Future { handleProgramForPeer[Client](bobNetwork)(chirperProtocol) }
    val carolFuture = Future { handleProgramForPeer[Client](carolNetwork)(chirperProtocol) }

    val all = Future.sequence(Seq(serverFuture, aliceFuture, bobFuture, carolFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("Chirper done.")
end Chirper
