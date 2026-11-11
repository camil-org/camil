package io.github.party

import scala.caps.unsafe.unsafeAssumePure
import Collective.*

object CollectiveBuildingBlocks:
  trait DistanceSensor:
    def nbrRange(using c: Collective, vm: VM): Field[Double]

  object DistanceSensor:
    def nbrRange(using c: Collective, vm: VM, sensor: DistanceSensor): Field[Double] = sensor.nbrRange
  
  def G[V](using vm: VM, c: Collective)(source: Boolean, initial: V, acc: V ->{c, vm} V, metric: () => Field[Double]) =
    val m = metric()
    rep((Double.MaxValue, initial)): dv =>
      mux(source)((0.0, initial)):
        val (d, v) = nbr((dv._1, dv._2))
          .combine(m)((tuple, metric) => (tuple._1 + metric, tuple._2))
          .withoutSelf
          .values
          .minByOption(_._1)
          .getOrElse((Double.MaxValue, dv._2))
        (d, acc(v))
    ._2

  def distanceTo(source: Boolean)(using vm: VM, c: Collective, sensor: DistanceSensor) =
    G(
      source, 
      initial = 0.0, 
      acc = _ + sensor.nbrRange.minWithoutSelf(Double.MaxValue), 
      metric = unsafeAssumePure(() => unsafeAssumePure(sensor.nbrRange))
    )

  def broadcast[V](source: Boolean, value: V)(using vm: VM, c: Collective, sensor: DistanceSensor) =
    G(source, initial = value, acc = identity, metric = unsafeAssumePure((() => unsafeAssumePure(sensor.nbrRange))))

  def distanceBetween(source: Boolean, target: Boolean)(using VM, Collective, DistanceSensor) =
    broadcast(source, distanceTo(target))

  def C[D: Ordering, V](using vm: VM, c: Collective)(
    potential: Field[D],
    local: V, 
    combine: (V, V) ->{c, vm} V,
    zero: V
  ): V =
    val neighborsParents = nbr(findParent(potential).getOrElse(vm.deviceId))
    rep(local): v =>
      val nValues = nbr(v)
      val childValues = neighborsParents.withoutSelf.collect:
        case (childId, parentId) if parentId == vm.deviceId =>
          nValues.withoutSelf
            .find(_._1 == childId)
            .map(_._2)
            .getOrElse(zero)
      .foldLeft(zero)(combine)
      combine(local, childValues)

  private def findParent[D: Ordering](potential: Field[D]): Option[potential.Id] =
    val myPotential = potential.localValue
    potential.withoutSelf
      .minByOption(_._2)
      .filter((_, d) => summon[Ordering[D]].lt(d, myPotential))
      .map(_._1)

  def S[D: Numeric](grain: D, metric: () => Field[D]): Boolean = ???
