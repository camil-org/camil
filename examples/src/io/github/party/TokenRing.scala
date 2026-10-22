package io.github.party

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import io.github.party.Multitier
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Identifier
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

object TokenRing:
  type Node <: { type Tie <: Multiple[Node] }

  type Id = UUID
  type Token = String
  type Envelope = (Id, Token)

  private val defaultRingSize = 5
  private val minEmitDelayMs = 2000L
  private val emitDelayPerNodeMs = 600L
  private val minCompletionTimeoutMs = 5000L
  private val completionTimeoutPerNodeMs = 3000L

  private def parseNodeCount(args: Array[String]): Int =
    args.headOption match
      case None => defaultRingSize
      case Some(raw) =>
        raw.toIntOption match
          case Some(count) => count
          case None =>
            println(s"[TokenRing] Invalid node count '$raw'; using default $defaultRingSize.")
            defaultRingSize

  private def predecessorOf(localAddress: String, ringMembers: Vector[String]): String =
    val index = ringMembers.indexOf(localAddress)
    if index < 0 then throw IllegalArgumentException(s"Peer '$localAddress' is not in ring members: ${ringMembers.mkString(", ")}")
    ringMembers((index - 1 + ringMembers.size) % ringMembers.size)

  private def signalKeyFor(address: String): Identifier =
    Identifier(
      id = s"party::token-ring::signal::$address",
      namespace = Some("signal"),
      metadata = Map("address" -> address),
    )

  def tokenRingProtocol(ringMembers: Vector[String], emitNotBeforeEpochMs: Long)(using Network, Placement, Multitier): Unit = Multitier:
    on[Node]:
      val network = summon[Network]
      val localPeerAddress = peerAddress
      val localAddress = localPeerAddress.toString
      val localId = UUID.randomUUID()
      val done = CountDownLatch(1)
      val seen = AtomicInteger(0)
      val completionTimeoutMs = math.max(minCompletionTimeoutMs, ringMembers.size.toLong * completionTimeoutPerNodeMs)

      val predecessorAddress = predecessorOf(localAddress, ringMembers)
      val predecessorSignalKey = signalKeyFor(predecessorAddress)
      val localSignalKey = signalKeyFor(localAddress)
      val predecessorPeerAddress = predecessorAddress.asInstanceOf[network.PeerAddress]
      given Raise[NetworkError] = Raise.rethrowError

      val (outboundSignal, outboundEmitter) = Signal.make[Envelope]
      network.registerSignal(localSignalKey, outboundSignal)

      val predecessorSignal = network.pull[Node, Node, Signal[Envelope]](predecessorPeerAddress, predecessorSignalKey)

      predecessorSignal.subscribe:
        case (receiver, token) =>
          val current = seen.incrementAndGet()
          if receiver == localId then println(s"""[$localAddress] received: "$token" on: $localId""")
          else outboundEmitter.emit((receiver, token))
          if current >= ringMembers.size then done.countDown()

      val waitBeforeEmitMs = emitNotBeforeEpochMs - System.currentTimeMillis()
      if waitBeforeEmitMs > 0 then Thread.sleep(waitBeforeEmitMs)
      outboundEmitter.emit(localId -> s"token for $localId")

      val completed = done.await(completionTimeoutMs, TimeUnit.MILLISECONDS)
      if !completed then println(s"[$localAddress] timeout waiting for ring completion (${seen.get()}/${ringMembers.size} tokens seen).")

      outboundEmitter.close()

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    val nodeCount = parseNodeCount(args)
    if nodeCount < 2 then
      System.err.println(s"[TokenRing] Invalid node count $nodeCount: ring requires at least 2 nodes.")
      return

    val ringMembers = (1 to nodeCount).map(index => s"node-$index").toVector
    val emitNotBeforeEpochMs = System.currentTimeMillis() + math.max(minEmitDelayMs, ringMembers.size.toLong * emitDelayPerNodeMs)

    println(s"Running TokenRing multitier port with ${ringMembers.size} nodes...")
    val broker = InMemoryNetwork.broker()
    val runEc = scala.concurrent.ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(nodeCount))

    val futures = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
    var idx = 0
    while idx < ringMembers.size do
      val net = InMemoryNetwork[Node](ringMembers(idx), broker)(using summon[PeerTag[Node]], global)
      futures += Future {
        handleProgramForPeer[Node](net)(tokenRingProtocol(ringMembers, emitNotBeforeEpochMs))
      }(using runEc)
      idx += 1

    try
      Await.result(Future.sequence(futures.toSeq), Duration.Inf)
      println("TokenRing done.")
    finally runEc.shutdown()
  end main
end TokenRing
