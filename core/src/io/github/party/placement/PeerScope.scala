package io.github.party.placement

import scala.caps.SharedCapability

import io.github.party.network.Identifier
import io.github.party.peers.Peers.Peer
import io.github.party.placement.PlacementType.{ on, PlacementValue }

trait PeerScope[+P <: Peer] extends SharedCapability:
  def id: Identifier

  def take[V](placement: V on P): V = placement.runtimeChecked match
    case PlacementValue.Local(value, _) => value

object PeerScope:
  def take[P <: Peer, V](using scope: PeerScope[P])(placement: V on P): V = scope.take(placement)
