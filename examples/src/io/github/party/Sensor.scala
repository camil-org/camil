package io.github.party

import io.github.party.Collective.nbr
import io.github.party.CollectiveBuildingBlocks.DistanceSensor

object NbrSensor:
  def sensor(positions: Map[String, (Double, Double)])(using Collective): DistanceSensor = new DistanceSensor:
    def nbrRange(using Collective, VM): Field[Double] =
      val localAddress = summon[VM].deviceId.asInstanceOf[String]
      val localPos = positions(localAddress)
      nbr(localPos).map { position =>
        math.sqrt(
          math.pow(localPos._1 - position._1, 2) +
            math.pow(localPos._2 - position._2, 2),
        )
      }
