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
      mux(source) {
        (0.0, initial)
      } {
        val (d, v) = nbr((dv._1, dv._2)).combine(m) { case (tuple, metric) =>
          (tuple._1 + metric, tuple._2)
        }.withoutSelf.values.minByOption(_._1).getOrElse((Double.MaxValue, dv._2))
        (d, acc(v))
      }
    ._2

  def distanceTo(source: Boolean)(using vm: VM, c: Collective, sensor: DistanceSensor) =
    G(source, 0.0, _ + sensor.nbrRange.minWithoutSelf(Double.MaxValue), unsafeAssumePure(() => unsafeAssumePure(sensor.nbrRange)))

  def broadcast[V](source: Boolean, value: V)(using vm: VM, c: Collective, sensor: DistanceSensor) =
    G(source, value, identity, unsafeAssumePure((() => unsafeAssumePure(sensor.nbrRange))))

  def distanceBetween(source: Boolean, target: Boolean)(using VM, Collective, DistanceSensor) =
    broadcast(source, distanceTo(target))