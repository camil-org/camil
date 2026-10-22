package io.github.party.handlers

import io.github.party.peers.Peers.PeerTag
import io.github.party.peers.Peers.Peer
import io.github.party.Collective
import io.github.party.VM
import io.github.party.Field
import io.github.party.network.Network

private[party] case class FieldImpl[+V, DeviceId](localId: DeviceId, override val localValue: V, values: Map[DeviceId, V]) extends Field[V]:
  type Id = DeviceId
  def map[U](f: V -> U): Field[U] = FieldImpl(localId, f(localValue), values.view.mapValues(f).toMap)

  def withoutSelf: Map[Id, V] = values.filterNot(_._1 == localId)

  def combine[A, B](that: Field[A])(f: (V, A) -> B): Field[B] =
    val local = f(this.localValue, that.localValue)
    val combinedValues = this.values.flatMap: 
      case (id: that.Id @unchecked, v) => that.withoutSelf.get(id).map(a => id -> f(v, a))
      case _ => None
    FieldImpl(localId, local, combinedValues.toMap)

private final class CollectiveHandlerImpl[P <: Peer: PeerTag, Id] extends Collective:
  def rep[V](using vm: VM)(initial: V)(evolution: V ->{this, vm} V): V =
    vm.align("rep")
    val result = evolution(vm.getStateAt(vm.currentPath).getOrElse(initial))
    vm.setStateAt(vm.currentPath, result)
    vm.dealign()
    result

  def nbr[V](using vm: VM)(value: V): Field[V] =
    vm.align("nbr")
    vm.setValueAt(vm.currentPath, value)
    val neighbors = vm.neighborValuesAt[V](vm.currentPath)
    vm.dealign()
    FieldImpl(vm.deviceId, value, neighbors)

  def branch[V](using vm: VM)(condition: Boolean)(trueBranch: ->{this, vm} V)(falseBranch: ->{this, vm} V): V =
    vm.align(s"branch[$condition]")
    val result = if condition then trueBranch else falseBranch
    vm.dealign()
    result

  def mux[V](using vm: VM)(condition: Boolean)(trueValue: V)(falseValue: V): V =
    if condition then trueValue else falseValue

object CollectiveHandler:
  def handle[P <: Peer: PeerTag, DeviceId, V](using n: Network): Collective = new CollectiveHandlerImpl[P, n.PeerAddress]
