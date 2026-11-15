package io.github.party

import io.github.party.Collective.nbr
import io.github.party.CollectiveBuildingBlocks.DistanceSensor

object NbrSensor:
  type Position = (Double, Double)

  def fromDevicePositions(positions: Map[String, Position])(using Collective): DistanceSensor =
    fromLocalPosition(localPosition = positions(summon[VM].deviceId.asInstanceOf[String]))

  def fromLocalPosition(localPosition: VM ?-> Position)(using Collective): DistanceSensor =
    new DistanceSensor:
      def nbrRange(using c: Collective, vm: VM): Field[vm.DeviceId, Double] =
        val resolvedPos = localPosition
        nbr(resolvedPos).map: position =>
          math.sqrt(
            math.pow(resolvedPos._1 - position._1, 2) +
              math.pow(resolvedPos._2 - position._2, 2),
          )
end NbrSensor
