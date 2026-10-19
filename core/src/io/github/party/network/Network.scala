package io.github.party.network

import scala.caps.SharedCapability

import io.github.party.network as PeerAddress
import io.github.party.peers.Peers.*
import io.github.party.raise.Raise
import io.github.party.signal.Signal

enum NetworkEvent[PeerAddress](from: PeerAddress):
  case ValueEmitted[V, PeerAddress](key: Identifier, value: V, from: PeerAddress, to: PeerAddress) extends NetworkEvent(from)
  case Subscribed(key: Identifier, from: PeerAddress) extends NetworkEvent(from)
  case Unsubscribed(key: Identifier, from: PeerAddress) extends NetworkEvent(from)
  case Close(key: Identifier, from: PeerAddress) extends NetworkEvent(from)

enum NetworkError(val message: String) extends Throwable(message):
  case SinglePeerExpected(peerType: String) extends NetworkError(s"Expected a single peer of type $peerType, but found multiple.")
  case UnreachablePeer(info: String) extends NetworkError(s"Unreachable peer: $info")
  case NetworkFailure(err: String) extends NetworkError(s"Network failure: $err")
  case KeyNotFound(key: Identifier) extends NetworkError(s"Key not found: $key")
  case RuntimeError[E](error: E) extends NetworkError(s"Runtime error: $error")

trait Network extends SharedCapability:
  type PeerAddress

  def reachablePeers: Set[PeerAddress]
  def reachablePeersOf[P <: Peer: PeerTag]: Set[PeerAddress]
  def peerAddress: PeerAddress

  /**
   * Push the value [[to]] the specified peer address. The [[value]] is associated with the provided [[key]] and can be retrieved by the receiving
   * peer using the same key.
   *
   * The function may fail with a [[NetworkError]].
   */
  def push[S <: TiedWith[D], D <: Peer, V](using Raise[NetworkError])(to: PeerAddress, key: Identifier, value: V): Unit

  /**
   * Pull the value associated with the provided [[key]] from the specified peer address. Typically this is a request-response interaction where the
   * requesting peer sends a request to the target peer, which then responds with the value associated with the key.
   *
   * The function may fail with a [[NetworkError]].
   */
  def pull[From <: TiedWith[To], To <: Peer, V](using Raise[NetworkError])(from: PeerAddress, key: Identifier): V

  /**
   * Pull the value associated with the provided [[key]] from all specified peer addresses. This is typically used in scenarios where multiple peers
   * are expected to provide a value for the same key, and the requesting peer wants to gather all those values.
   *
   * The function may fail with a [[NetworkError]].
   */
  def pullFromAll[From <: TiedWith[To], To <: Peer, V](using Raise[NetworkError])(from: Set[PeerAddress], key: Identifier): Map[PeerAddress, V]

  /**
   * Propagate the value associated with the provided [[key]] to all reachable peers.
   *
   * The function may fail with a [[NetworkError]].
   */
  def broadcast[S <: Peer, V](using Raise[NetworkError])(key: Identifier, value: V): Unit

  /**
   * Given the provided [[key]], look up the value in the local store and return it if found. If the value is not found, a
   * [[NetworkError.KeyNotFound]] error is raised.
   *
   * Generally, this method implements a retry mechanism to synchronize with the remote peer, ensuring that the value is eventually retrieved. The
   * retry mechanism is responsibility of the network implementation and may involve strategies such as exponential backoff or fixed intervals.
   */
  def retrieve[S <: Peer, V](using Raise[NetworkError])(key: Identifier): V

  def retrieveNow[S <: Peer, V](using Raise[NetworkError])(from: PeerAddress, key: Identifier): Option[V]

  /**
   * Persist the value associated with the provided [[key]] in the local store. This allows the value to be retrieved later using the same key, either
   * locally or by remote peers through pull requests.
   */
  def store[V](key: Identifier, value: V): Unit

  // Collective
  def storeValueTree[P <: Peer, V](forPeer: PeerAddress, key: Identifier, valueTree: V): Unit
  def retrieveValueTree[P <: Peer, V](peer: PeerAddress, key: Identifier): Option[V]

  // Reactive primitives

  /**
   * Registers into the network a [[signal]] identified by the provided [[key]]. Internally the network listens for emitted values and propagates them
   * to all subscribed peers.
   */
  def registerSignal[V](key: Identifier, signal: Signal[V]): Unit

  def retrieveSignal[V](key: Identifier): Signal[V]

  def emitLocalSignal[V](to: PeerAddress, key: Identifier, value: V): Unit

  def receiveRemoteSignalValue[V](key: Identifier, value: V): Unit

  def subscribe(peerAddress: PeerAddress, key: Identifier): Unit

  def unsubscribe(peerAddress: PeerAddress, key: Identifier): Unit
end Network

object Network:
  def peerAddress(using n: Network): n.PeerAddress = n.peerAddress

  def reachablePeersOf[P <: Peer: PeerTag](using n: Network): Set[n.PeerAddress] = n.reachablePeersOf[P]
