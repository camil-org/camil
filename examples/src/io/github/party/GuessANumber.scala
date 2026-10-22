package io.github.party

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
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

object GuessANumber:
  type Server <: { type Tie <: Single[Client] }
  type Client <: { type Tie <: Single[Server] }

  final case class ServerState(secret: Int, triesLeft: Int, finished: Boolean)
  final case class GuessResult(guess: Int, message: String, triesLeft: Int, terminal: Boolean)

  private val minGuess = 1
  private val maxGuess = 10
  private val maxTries = 3
  private val secret = 7
  private val scriptedGuesses = List(0, 4, 12, 7)
  private val betweenGuessesDelayMs = 150L

  private def evaluateGuess(guess: Int, state: ServerState): (ServerState, GuessResult) =
    if state.finished then (state, GuessResult(guess, "Game already finished", state.triesLeft, terminal = true))
    else if guess < minGuess || guess > maxGuess then (state, GuessResult(guess, s"Out of range: $guess", state.triesLeft, terminal = false))
    else if state.triesLeft > 0 && guess == state.secret then
      val next = state.copy(triesLeft = 0, finished = true)
      (next, GuessResult(guess, "You win!", next.triesLeft, terminal = true))
    else if state.triesLeft <= 1 then
      val next = state.copy(triesLeft = 0, finished = true)
      (next, GuessResult(guess, "Game over", next.triesLeft, terminal = true))
    else
      val next = state.copy(triesLeft = state.triesLeft - 1)
      (next, GuessResult(guess, "Try again", next.triesLeft, terminal = false))

  def guessANumberProtocol(using Network, Placement, Multitier): Unit = Multitier:
    val guessesOnClient = on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      signalBuilder[Int]: emitter =>
        scriptedGuesses.foreach: guess =>
          Thread.sleep(betweenGuessesDelayMs)
          println(s"[Client $clientId] submitted guess: $guess")
          emitter.emit(guess)

    val responsesOnServer = on[Server]:
      val incomingGuesses = asLocal[Server, Client](guessesOnClient)
      val (responseSignal, responseEmitter) = Signal.make[GuessResult]
      val stateRef = AtomicReference(ServerState(secret, maxTries, finished = false))
      val isClosed = AtomicBoolean(false)

      def closeResponses(): Unit =
        if isClosed.compareAndSet(false, true) then
          println("[Server] Closing response stream.")
          responseEmitter.close()

      incomingGuesses.subscribe: guess =>
        val current = stateRef.get()
        val (next, result) = evaluateGuess(guess, current)
        stateRef.set(next)
        println(s"[Server] guess=$guess -> '${result.message}' (triesLeft=${result.triesLeft})")
        responseEmitter.emit(result)
        if result.terminal then closeResponses()

      incomingGuesses.onClose: () =>
        closeResponses()

      responseSignal

    on[Client]:
      val clientId = peerAddress.asInstanceOf[String]
      val incomingResults = asLocal[Client, Server](responsesOnServer)
      val done = CountDownLatch(1)

      incomingResults.subscribe: result =>
        println(
          s"[Client $clientId] result for guess ${result.guess}: ${result.message} (triesLeft=${result.triesLeft})",
        )

      incomingResults.onClose: () =>
        println(s"[Client $clientId] server stream closed.")
        done.countDown()

      done.await()
  end guessANumberProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running GuessANumber Section 3.2 port...")
    val broker = InMemoryNetwork.broker()
    val serverNetwork = InMemoryNetwork[Server]("server", broker)
    val clientNetwork = InMemoryNetwork[Client]("client", broker)

    val serverFuture = Future { handleProgramForPeer[Server](serverNetwork)(guessANumberProtocol) }
    val clientFuture = Future { handleProgramForPeer[Client](clientNetwork)(guessANumberProtocol) }

    val all = Future.sequence(Seq(serverFuture, clientFuture))
    Await.result(all, scala.concurrent.duration.Duration.Inf)
    println("GuessANumber done.")
end GuessANumber
