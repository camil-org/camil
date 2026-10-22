package io.github.party.handlers

import io.github.party.placement.{PlacementType, PeerScope}
import io.github.party.placement.PlacementType.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.network.Identifier
import io.github.party.network.Network
import io.github.party.network.NetworkError
import io.github.party.raise.Raise
import io.github.party.utils.Utils.select
import io.github.party.signal.Signal
import scala.reflect.ClassTag

private final class PlacementTypeHandler[L <: Peer: PeerTag] extends PlacementType:
  private var counter = -1
  update def on[P <: Peer: PeerTag, V: ClassTag, C^](using Network)(body: PeerScope[P] ?=> V^{C}): V^{C} on P =
    val net = summon[Network]
    val local = summon[PeerTag[L]]
    val placedPeer = summon[PeerTag[P]]
    val tag = summon[ClassTag[V]]
    val key = if tag.runtimeClass.isAssignableFrom(classOf[Signal[?]]) then
      freshKey[P](Some("signal"), Map.empty)
    else
      freshKey[P](None, Map.empty)

    select(local, placedPeer)(
      onLocal = {
        given PeerScope[P] = new PeerScope[P] {
          def id: Identifier = key
        }
        val result = body
        result match
          case signal: Signal[_] =>
            net.registerSignal[V](key, signal.asInstanceOf[Signal[V]]) // Cast needed due to type erasure, but we ensure type safety by only allowing Signal[_] to be registered
          case value => net.store(key, value)
        PlacementValue.Local(result, key)
      },
      onRemote = PlacementValue.Remote(key)
    )

  override protected[party] update def freshKey[P <: Peer](namespace: Option[String] = None, metadata: Map[String, String] = Map.empty): Identifier =
    counter += 1
    Identifier(
      id = s"party::placement::${namespace.getOrElse("default")}::${metadata.map { case (k, v) => s"$k=$v" }.mkString(",")}::${counter}",
      namespace = namespace,
      metadata = metadata
    )

  protected[party] def local[V, P <: Peer](value: V, key: Identifier): V on P = PlacementValue.Local(value, key)
  protected[party] def remote[V, P <: Peer](key: Identifier): V on P = PlacementValue.Remote(key)
  protected[party] def getKey[V, P <: Peer](value: V on P): Identifier = value.key
  protected[party] def getLocalValue[V, P <: Peer](value: V on P): Option[V]  = value match
    case PlacementValue.Local(v, _) => Some(v)
    case _ => None

object PlacementTypeHandler:
  def run[L <: Peer: PeerTag, A](using r: Raise[NetworkError])(program: PlacementType^ ?=> A): A =
    given (PlacementType^) = PlacementTypeHandler[L]()
    program

  def handler[L <: Peer: PeerTag]: PlacementType^ = PlacementTypeHandler[L]()
