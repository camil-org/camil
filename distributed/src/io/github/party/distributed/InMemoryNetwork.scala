package io.github.party.distributed

import java.util.concurrent.{ ConcurrentLinkedQueue, LinkedBlockingQueue }

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.duration.*

import io.github.party.network.NetworkEvent
import io.github.party.network.{ Identifier, Network, NetworkError }
import io.github.party.peers.Peers.{ Peer, PeerTag, TiedWith }
import io.github.party.raise.Raise
import io.github.party.raise.Raise.ensure
import io.github.party.raise.Raise.raise
import io.github.party.signal.Emitter
import io.github.party.signal.Signal
import io.github.party.signal.Signal.SignallingImpl

/**
 * In-memory network implementation simulating distributed peers communication.
 *
 * Peers interact exclusively through a shared [[NetworkBroker]] which acts as a passive, static message/value store. The broker never holds
 * references to network instances — it only stores mailboxes, values, and metadata. Each peer reads from its own mailbox in the broker and writes
 * into others'.
 */
private final class InMemoryNetworkImpl[LocalPeer <: Peer: PeerTag](
    private val address: String,
    private val broker: NetworkBroker,
    private val timeout: FiniteDuration = 2.seconds,
)(using ExecutionContext)
    extends Network:
  private val localEmitters: TrieMap[Identifier, (Emitter[Any], Signal[Any])] = TrieMap.empty
  private val localEmittersPerSource: TrieMap[(PeerAddress, Identifier), (Emitter[Any], Signal[Any])] = TrieMap.empty
  private val remoteSignalSubscriptions: TrieMap[Identifier, mutable.Set[PeerAddress]] = TrieMap.empty

  type PeerAddress = String

  override def reachablePeers: Set[PeerAddress] = broker.getAllPeers.filterNot(_ == address)

  override def reachablePeersOf[P <: Peer: PeerTag]: Set[PeerAddress] =
    val targetPeerTag = summon[PeerTag[P]]
    broker.getPeersOfType(targetPeerTag.baseTypeRepr).filterNot(_ == address)

  override def peerAddress: PeerAddress = address

  override def push[S <: TiedWith[D], D <: Peer, V](using
      Raise[NetworkError],
  )(
      to: PeerAddress,
      key: Identifier,
      value: V,
  ): Unit =
    ensure(broker.hasPeer(to)) { NetworkError.UnreachablePeer(s"Cannot reach peer $to") }
    broker.putValue(to, key, value)

  override def pull[From <: TiedWith[To], To <: Peer, V](using
      Raise[NetworkError],
  )(
      from: PeerAddress,
      key: Identifier,
  ): V =
    val scope = key.namespace
    if Some("signal") == scope then
      broker.subscribe(to = from, origin = address, key)
      retrieveSignal(key).asInstanceOf[V]
    else
      ensure(broker.hasPeer(from)) { NetworkError.UnreachablePeer(s"Cannot reach peer $from") }
      // Request-response pattern: send a request to the remote peer and wait for the response
      val promiseResult = Promise[Any]()
      broker.putPendingRequest(from, key, promiseResult)
      val future = promiseResult.future
      Raise.catchNonFatal[NetworkError, V] {
        Await.result(future, timeout).asInstanceOf[V]
      }(err => NetworkError.NetworkFailure(s"Failed to pull value from $from for key $key: ${err.getMessage}"))

  override def pullFromAll[From <: TiedWith[To], To <: Peer, V](using Raise[NetworkError])(from: Set[String], key: Identifier): Map[String, V] =
    val scope = key.namespace
    if Some("signal") == scope then
      from.map { peer =>
        broker.subscribe(to = peer, origin = address, key)
        val signal = localEmittersPerSource
          .getOrElseUpdate(
            (peer, key), {
              val sig = new SignallingImpl[Any]
              (sig, sig)
            },
          )
          ._2
        peer -> signal.asInstanceOf[V]
      }.toMap
    else
      ensure(from.nonEmpty) { NetworkError.UnreachablePeer("No peer addresses provided for pullFromAll") }
      from.map { peerAddress =>
        ensure(broker.hasPeer(peerAddress)) { NetworkError.UnreachablePeer(s"Cannot reach peer $peerAddress") }
        val promiseResult = Promise[Any]()
        broker.putPendingRequest(peerAddress, key, promiseResult)
        val future = promiseResult.future
        val value = Raise.catchNonFatal[NetworkError, V] {
          Await.result(future, timeout).asInstanceOf[V]
        }(err => NetworkError.NetworkFailure(s"Failed to pull value from $peerAddress for key $key: ${err.getMessage}"))
        peerAddress -> value
      }.toMap
    end if
  end pullFromAll

  override def broadcast[S <: Peer, V](using Raise[NetworkError])(key: Identifier, value: V): Unit =
    broker.getAllPeers.foreach(broker.putValue(_, key, value))

  override def retrieve[S <: Peer, V](using Raise[NetworkError])(key: Identifier): V =
    // Look up value in this peer's store in the broker
    val maxRetries = 10
    val baseDelay = 50.millis
    @tailrec def tryRetrieve(attempt: Int): V =
      broker.getValue(address, key) match
        case Some(value) => value.asInstanceOf[V]
        case None if attempt >= maxRetries => raise(NetworkError.KeyNotFound(key))
        case None =>
          Thread.sleep(baseDelay.toMillis * Math.pow(1.5, attempt).toLong)
          tryRetrieve(attempt + 1)
    tryRetrieve(0)

  override def retrieveNow[S <: Peer, V](using Raise[NetworkError])(from: PeerAddress, key: Identifier): Option[V] =
    broker.getValue(from, key).map(_.asInstanceOf[V])

  override def store[V](key: Identifier, value: V): Unit = broker.putValue(address, key, value)

  // ---- Collective Ops ----
  override def retrieveValueTree[P <: Peer, V](from: String, key: Identifier): Option[V] =
    broker.getCollectiveValueTree(from, peerAddress, key).map(_.asInstanceOf[V])

  override def storeValueTree[P <: Peer, V](forPeer: String, key: Identifier, valueTree: V): Unit =
    broker.putCollectiveValueTree(peerAddress, forPeer, key, valueTree)

  // ---- Reactive primitives ----

  override def registerSignal[V](key: Identifier, signal: Signal[V]): Unit =
    signal.subscribe(value =>
      remoteSignalSubscriptions.get(key).foreach { subscribers =>
        subscribers.foreach { subscriber =>
          // println(s"Peer $address emitting signal value for key $key to subscriber $subscriber")
          emitLocalSignal(subscriber, key, value)
        }
      },
    )
    signal.onClose(() =>
      remoteSignalSubscriptions.get(key).foreach { subscriber =>
        subscriber.foreach(broker.closeSignal(peerAddress, _, key))
      },
    )

  override def retrieveSignal[V](key: Identifier): Signal[V] =
    localEmitters
      .getOrElseUpdate(
        key, {
          val signal = new SignallingImpl[V]
          (signal.asInstanceOf[Emitter[Any]], signal.asInstanceOf[Signal[Any]])
        },
      )
      ._2
      .asInstanceOf[Signal[V]]

  override def emitLocalSignal[V](to: PeerAddress, key: Identifier, value: V): Unit =
    broker.propagateSignalTo(peerAddress, to, key, value)

  override def receiveRemoteSignalValue[V](key: Identifier, value: V): Unit =
    localEmitters.get(key) match
      case Some((emitter, _)) => emitter.emit(value)
      case None => // No local emitter, ignore the value

  override def subscribe(peerAddress: String, key: Identifier): Unit =
    val subscribers = remoteSignalSubscriptions.getOrElseUpdate(key, mutable.Set.empty)
    subscribers += peerAddress

  override def unsubscribe(peerAddress: String, key: Identifier): Unit =
    val subscribers = remoteSignalSubscriptions.getOrElseUpdate(key, mutable.Set.empty)
    subscribers -= peerAddress

  @tailrec private def eventLoopProcessor(): Unit =
    broker
      .getEvents(peerAddress)
      .foreach:
        case NetworkEvent.Subscribed(key, from) => subscribe(from, key)
        case NetworkEvent.Unsubscribed(key, from) => unsubscribe(from, key)
        case NetworkEvent.ValueEmitted(key, value, from, to) =>
          localEmittersPerSource.get((from, key)) match
            case Some((emitter, _)) => emitter.emit(value)
            case None => receiveRemoteSignalValue(key, value)
        case NetworkEvent.Close(key, from) =>
          localEmittersPerSource.get((from, key)) match
            case Some((emitter, _)) => emitter.close()
            case None => localEmitters.get(key).foreach(_._1.close())
    Thread.sleep(10)
    eventLoopProcessor()

  summon[ExecutionContext].execute(() => eventLoopProcessor())
end InMemoryNetworkImpl

/**
 * A static, passive message broker that stores values and signal emissions.
 *
 * The broker has '''no references''' to any network instance. It only maintains:
 *   - Per-peer key-value stores (mailboxes for placed values)
 *   - Peer type metadata for discovery
 *   - Signal subscription metadata and emission mailboxes
 *
 * Peers write into the broker and read from it — the broker never pushes.
 */
class NetworkBroker:
  type PeerAddress = String
  // Per-peer value stores: peerAddress -> (key -> value)
  private val peerStores: TrieMap[String, TrieMap[Identifier, Any]] = TrieMap.empty

  private val collectiveValueTrees: TrieMap[PeerAddress, TrieMap[PeerAddress, Any]] = TrieMap.empty

  // Peer type metadata: peerAddress -> peerTypeName
  private val peerTypes: TrieMap[String, String] = TrieMap.empty

  // Stores all the signalling events for each peer (queue preserves insertion order)
  private val events: TrieMap[PeerAddress, ConcurrentLinkedQueue[NetworkEvent[PeerAddress]]] = TrieMap.empty

  // Pending requests: we use a synchronized wrapper object as both the queue holder and the lock
  private case class PendingRequestQueue(queue: LinkedBlockingQueue[Promise[Any]] = LinkedBlockingQueue())
  private val pendingRequests: TrieMap[(PeerAddress, Identifier), PendingRequestQueue] = TrieMap.empty

  private def getRequestQueue(peerAddress: PeerAddress, key: Identifier): PendingRequestQueue =
    pendingRequests.getOrElseUpdate((peerAddress, key), PendingRequestQueue())

  /** Register a peer address with its type. No network reference is stored. */
  def registerPeer[P <: Peer: PeerTag](address: PeerAddress): Unit =
    peerStores.getOrElseUpdate(address, TrieMap.empty)
    peerTypes.put(address, summon[PeerTag[P]].baseTypeRepr)

  /** Unregister a peer, removing all its stored data. */
  def unregisterPeer(address: PeerAddress): Unit =
    peerStores.remove(address)
    peerTypes.remove(address)

  /** Check whether a peer is registered. */
  def hasPeer(address: PeerAddress): Boolean =
    peerStores.contains(address)

  /** Get all registered peer addresses. */
  def getAllPeers: Set[PeerAddress] =
    peerStores.keySet.toSet

  /** Get all peer addresses registered with the given type name. */
  def getPeersOfType(peerType: String): Set[PeerAddress] =
    peerTypes.filter(_._2 == peerType).keySet.toSet

  /** Store a value in a peer's mailbox, keyed by [[Identifier]]. */
  def putValue(peerAddress: PeerAddress, key: Identifier, value: Any): Unit =
    val requestQueue = getRequestQueue(peerAddress, key)
    requestQueue.synchronized:
      // Store the value first
      peerStores.getOrElseUpdate(peerAddress, TrieMap.empty).put(key, value)
      // Then resolve any pending promises
      val promises = Iterator.continually(requestQueue.queue.poll()).takeWhile(_ != null).toList
      promises.foreach(_.success(value))
      if promises.nonEmpty then pendingRequests.remove((peerAddress, key))

  /** Retrieve a value from a peer's store. Returns [[None]] if not yet available. */
  def getValue(peerAddress: PeerAddress, key: Identifier): Option[Any] = synchronized:
    peerStores.get(peerAddress).flatMap(_.get(key))

  /** Store a pending request for a value that is not yet available. The promise will be completed when the value is put into the store. */
  def putPendingRequest(peerAddress: PeerAddress, key: Identifier, promise: Promise[Any]): Unit =
    val requestQueue = getRequestQueue(peerAddress, key)
    requestQueue.synchronized:
      getValue(peerAddress, key) match
        case Some(value) => promise.success(value)
        case None => requestQueue.queue.put(promise)

  // ---- Collective value tree management -----
  def putCollectiveValueTree(from: PeerAddress, to: PeerAddress, key: Identifier, valueTree: Any): Unit =
    val peerTrees = collectiveValueTrees.getOrElseUpdate(to, TrieMap.empty)
    peerTrees.put(from, valueTree)

  def getCollectiveValueTree(from: PeerAddress, local: PeerAddress, key: Identifier): Option[Any] =
    collectiveValueTrees.get(local).flatMap(_.get(from))

  // ---- Signal subscription management ----
  def subscribe(to: PeerAddress, origin: PeerAddress, key: Identifier): Unit =
    val event = NetworkEvent.Subscribed(key, origin)
    events.getOrElseUpdate(to, ConcurrentLinkedQueue()).add(event)

  def unsubscribe(to: PeerAddress, origin: PeerAddress, key: Identifier): Unit =
    val event = NetworkEvent.Unsubscribed(key, origin)
    events.getOrElseUpdate(to, ConcurrentLinkedQueue()).add(event)

  def propagateSignalTo[V](from: PeerAddress, to: PeerAddress, key: Identifier, value: V): Unit =
    val event = NetworkEvent.ValueEmitted(key, value, from, to)
    events.getOrElseUpdate(to, ConcurrentLinkedQueue()).add(event)

  def closeSignal(from: PeerAddress, to: PeerAddress, key: Identifier): Unit =
    val closingEvent = NetworkEvent.Close(key, from)
    events.getOrElseUpdate(to, ConcurrentLinkedQueue()).add(closingEvent)

  def getEvents(peerAddress: PeerAddress): Seq[NetworkEvent[PeerAddress]] =
    events.get(peerAddress) match
      case Some(queue) =>
        val batch = mutable.ArrayBuffer.empty[NetworkEvent[PeerAddress]]
        var event = queue.poll()
        while event != null do
          batch += event
          event = queue.poll()
        batch.toSeq
      case None => Seq.empty

end NetworkBroker

object InMemoryNetwork:
  /**
   * Create a network for simulating multiple distributed peers.
   *
   * @param address
   *   the unique address identifying this peer
   * @param broker
   *   the shared (static) broker used as a passive message store
   * @return
   *   a network instance for the given peer
   */
  def apply[P <: Peer: PeerTag](address: String, broker: NetworkBroker)(using ExecutionContext): Network =
    broker.registerPeer[P](address)
    InMemoryNetworkImpl[P](address, broker)

  /** Create a network broker for coordinating multiple peers. */
  def broker(): NetworkBroker = NetworkBroker()

  // /** Convenience method to create a network with a new broker. */
  // def standalone[P <: Peer: PeerTag](address: String): (Network, NetworkBroker) =
  //   val b = broker()
  //   val net = apply[P](address, b)
  //   (net, b)
end InMemoryNetwork
