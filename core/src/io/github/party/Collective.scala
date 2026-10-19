package io.github.party

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable
import scala.caps.Stateful

import io.github.party.peers.Peers.{ Peer, PeerTag, TiedManyWith }
import io.github.party.signal.Signal
import io.github.party.signal.Signal.signalBuilder
import io.github.party.placement.{ Placement, PlacementType, PeerScope }
import io.github.party.placement.PlacementType.on
import io.github.party.network.{ Network, NetworkError, Identifier }
import io.github.party.raise.Raise

trait VM extends Stateful:
  type ValueTree = Map[String, Any]
  type Path = String
  type DeviceId

  private var context: Map[Path, Map[DeviceId, Any]] = Map.empty
  private val messages = mutable.Map.empty[Path, Any]
  private val state = mutable.Map.empty[Path, Any]

  private val stack = mutable.Stack.empty[InvocationCoordinate]
  private val trace = mutable.Map.empty[Path, Int]

  private case class InvocationCoordinate(key: String, invocationCount: Int):
    override def toString(): String = s"$key#$invocationCount"

  def deviceId: DeviceId

  def align(token: String): Unit =
    val invocationCount = trace.get(currentPath).map(_ + 1).getOrElse(0)
    stack.push(InvocationCoordinate(token, invocationCount))

  def dealign(): Unit =
    if stack.nonEmpty then
      val coord = stack.pop()
      trace.update(currentPath, coord.invocationCount)
    else throw new RuntimeException("Dealign called without a matching align")

  def currentPath: Path = stack.reverse.mkString("/")

  def setStateAt[V](path: Path, value: V): Unit = state.update(path, value)
  def getStateAt[V](path: Path): Option[V] = state.get(path).map(_.asInstanceOf[V])
  def getState: Map[Path, Any] = state.toMap

  def neighborValuesAt[V](path: Path): Map[DeviceId, V] = 
    context.get(path)
      .map(_.view.mapValues(_.asInstanceOf[V]).toMap)
      .getOrElse(Map.empty)

  def setValueAt[V](path: Path, value: V): Unit = messages.update(path, value)

  def outbound: ValueTree = messages.toMap

  update def prepareInbound(neighborsValueTrees: Map[DeviceId, ValueTree], prevState: Map[Path, Any]): Unit =
    resetVm()
    state ++= prevState
    context = Map.empty
    neighborsValueTrees.foreach: (neighborId, valueTree) =>
      valueTree.foreach: (path, value) =>
        val pathMap = context.getOrElse(path, Map.empty[DeviceId, Any])
        context = context + (path -> (pathMap + (neighborId -> value)))

  update private def resetVm(): Unit =
    context = Map.empty
    messages.clear()
    state.clear()
    stack.clear()
    trace.clear()

trait Field[+V]:
  type Id

  def withoutSelf: Map[Id, V]
  def map[U](f: V -> U): Field[U]
  def localValue: V
  def combine[A, B](that: Field[A])(f: (V, A) -> B): Field[B]

object Field:
  extension [N: Numeric](field: Field[N])
    def sum: N = field.withoutSelf.values.fold(field.localValue)(Numeric[N].plus)
    def sumWithoutSelf(default: N): N = field.withoutSelf.values.fold(default)(Numeric[N].plus)
    def max: N = field.maxWithoutSelf(field.localValue)
    def maxWithoutSelf(default: N): N = field.withoutSelf.values.fold(default)(Numeric[N].max)
    def min: N = field.minWithoutSelf(field.localValue)
    def minWithoutSelf(default: N): N = field.withoutSelf.values.fold(default)(Numeric[N].min)
    def +[U >: N: Numeric](that: Field[U]): Field[U] = field.combine(that)(Numeric[U].plus)
    def -[U >: N: Numeric](that: Field[U]): Field[U] = field.combine(that)(Numeric[U].minus)
    def *[U >: N: Numeric](that: Field[U]): Field[U] = field.combine(that)(Numeric[U].times)

trait Collective extends Multiparty:
  def rep[V](using VM)(initial: V)(evolution: V ->{this} V): V
  def nbr[V](using VM)(value: V): Field[V]
  def branch[V](using VM)(condition: Boolean)(trueBranch: ->{this} V)(falseBranch: ->{this} V): V
  def mux[V](using VM)(condition: Boolean)(trueValue: V)(falseValue: V): V

object Collective:
  def rep[V](using c: Collective, vm: VM)(initial: V)(evolution: V ->{c} V): V = c.rep(initial)(evolution)
  def nbr[V](using c: Collective, vm: VM)(value: V): Field[V] = c.nbr(value)
  def branch[V](using c: Collective, vm: VM)(condition: Boolean)(trueBranch: ->{c} V)(falseBranch: ->{c} V): V = c.branch(condition)(trueBranch)(falseBranch)
  def mux[V](using c: Collective, vm: VM)(condition: Boolean)(trueValue: V)(falseValue: V): V = c.mux(condition)(trueValue)(falseValue)

  def apply[P <: TiedManyWith[P]: PeerTag](using
    n: Network,
    p: PlacementType^,
    c: Collective,
    ec: ExecutionContext
  )[V](round: FiniteDuration)(program: VM^ ?->{c} V): Signal[V] on P = on[P]:
    type LocalVM = VM { type DeviceId = n.PeerAddress }
    given vm: (LocalVM^) = new VM:
      type DeviceId = n.PeerAddress
      override def deviceId = n.peerAddress
    val scopeId = summon[PeerScope[P]].id
    val key: Identifier = scopeId.copy(namespace = Some("collective"), metadata = Map("valuetree" -> "true"))
    signalBuilder: signal =>
      while true do
        val neighbors = n.reachablePeersOf[P]
        val neighborValues = neighbors
          .map(neighbor => neighbor -> n.retrieveValueTree[P, vm.ValueTree](neighbor, key))
          .collect { case (neighbor, Some(valueTree)) => neighbor -> valueTree }
          .toMap
        vm.prepareInbound(neighborValues, vm.getState)
        val result = program(using vm)
        val outboundValueTree = vm.outbound
        neighbors.foreach(neighbor => n.storeValueTree[P, vm.ValueTree](neighbor, key, outboundValueTree))
        signal.emit(result)
        Thread.sleep(round.toMillis)
