package io.github.party.handlers

import io.github.party.peers.Peers.{Peer, PeerTag}
import io.github.party.network.{Network, NetworkError, Identifier}
import io.github.party.Multitier
import io.github.party.raise.Raise
import io.github.party.placement.PlacementType
import io.github.party.peers.Peers.TiedManyWith
import io.github.party.peers.Peers.TiedSingleWith
import io.github.party.raise.Raise.ensure
import io.github.party.signal.Signal
import java.util.concurrent.ConcurrentLinkedQueue
import io.github.party.placement.PlacementType.on

private final class MultitierEffectImpl[P <: Peer: PeerTag](using r: Raise[NetworkError]) extends Multitier:
  type |~>[Id, V] = Map[Id, V]

  extension [Id, V](perPeer: Id |~> V) 
    def get(peerId: Id): Option[V] = perPeer.get(peerId)
    def getOrElse(peerId: Id, default: -> V): V = perPeer.getOrElse(peerId, default)
    def mapValues[U](f: V -> U): Id |~> U = perPeer.view.mapValues(f).toMap
    def fold[U](initial: U)(f: (U, Id, V) -> U): U = 
      perPeer.foldLeft(initial):
        case (acc, (id, value)) => f(acc, id, value)
    def values: Iterable[V] = perPeer.values
    def toMap: Map[Id, V] = perPeer

  def asLocal[L <: TiedSingleWith[R]: PeerTag, R <: Peer: PeerTag, V](using n: Network)(placement: V on R): V =
    val key = placement.key
    val reachablePeers = n.reachablePeersOf[R]
    ensure(reachablePeers.size == 1):
      NetworkError.SinglePeerExpected(s"Expected exactly one reachable peer of type ${summon[PeerTag[R]]}, but found ${reachablePeers.size}")
    val remotePeer = reachablePeers.head
    n.pull(remotePeer, key)

  def asLocalAll[L <: TiedManyWith[R]: PeerTag, R <: Peer: PeerTag, V](using n: Network)(placement: V on R): n.PeerAddress |~> V =
    val key = placement.key
    val reachablePeers = n.reachablePeersOf[R]
    ensure(reachablePeers.nonEmpty):
      NetworkError.SinglePeerExpected(s"Expected at least one reachable peer of type ${summon[PeerTag[R]]}, but found none")
    n.pullFromAll(reachablePeers, key)

object MultitierHandler:
  def run[P <: Peer: PeerTag](using r: Raise[NetworkError])[V](program: Multitier^ ?=> V): V =
    given (Multitier^{r}) = new MultitierEffectImpl[P]
    program

  def handler[P <: Peer: PeerTag](using Raise[NetworkError]): Multitier = new MultitierEffectImpl[P]
