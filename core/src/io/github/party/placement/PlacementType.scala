package io.github.party.placement

import io.github.party.network.{Identifier, Network}
import io.github.party.peers.Peers
import io.github.party.placement.PeerScope
import io.github.party.peers.Peers.*
import PlacementType.*
import scala.caps.Mutable
import io.github.party.network.Identifier
import io.github.party.network.Network
import scala.caps.ExclusiveCapability
import scala.annotation.targetName
import scala.reflect.ClassTag

type Placement = PlacementType^

trait PlacementType extends Mutable:
  update def on[P <: Peer: PeerTag, V: ClassTag, C^](using Network)(body: PeerScope[P] ?=> V^{C}): V^{C} on P

  protected[party] update def freshKey[P <: Peer](namespace: Option[String] = None, metadata: Map[String, String] = Map.empty): Identifier

  protected[party] def local[V, P <: Peer](value: V, key: Identifier): V on P
  protected[party] def remote[V, P <: Peer](key: Identifier): V on P
  protected[party] def getKey[V, P <: Peer](value: V on P): Identifier
  protected[party] def getLocalValue[V, P <: Peer](value: V on P): Option[V]

object PlacementType:
  infix type on[+V, -P <: Peer] = PlacementValue[V, P]

  protected[party] enum PlacementValue[+V, -P <: Peer](val key: Identifier):
    case Local(value: V, override val key: Identifier) extends PlacementValue[V, P](key)
    case Remote(override val key: Identifier) extends PlacementValue[V, P](key)

  def on[P <: Peer: PeerTag](using pt: PlacementType^, n: Network)[V: ClassTag, C^](body: PeerScope[P] ?=> V^{C}): V^{C} on P =
    pt.on(body)

  def place[P <: Peer, V](value: V): V on P =
    val key = Identifier(s"party::placement::value::${java.util.UUID.randomUUID()}")
    PlacementValue.Local(value, key)
