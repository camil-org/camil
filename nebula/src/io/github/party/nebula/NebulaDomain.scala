package io.github.party.nebula

object NebulaDomain:
  final case class Model(weights: Vector[Double])

  final case class LocalDataset(samples: Int, gradient: Vector[Double], position: (Double, Double))

  final case class LocalMetrics(loss: Double, accuracy: Double)

  final case class TopologyStatus(
      nodeId: String,
      closeNeighbors: Int,
      avgDistance: Double,
      distanceToSeed: Double,
      stability: Double,
      connected: Boolean,
      eligible: Boolean,
  )

  final case class LocalUpdate(
      nodeId: String,
      round: Int,
      samples: Int,
      model: Model,
      metrics: LocalMetrics,
  )

  final case class RoundSummary(
      round: Int,
      contributors: List[String],
      dropped: List[String],
      aggregated: Model,
  )

  def trainLocally(base: Model, dataset: LocalDataset, round: Int): (Model, LocalMetrics) =
    val roundFactor = 1.0 / round.toDouble
    val sampleBoost = dataset.samples.toDouble / 1000.0
    val updatedWeights = base.weights
      .zip(dataset.gradient)
      .map:
        case (weight, gradient) =>
          weight - (gradient * 0.18 * roundFactor) + sampleBoost

    val model = Model(updatedWeights)
    val l1 = updatedWeights.map(math.abs).sum / updatedWeights.size.toDouble
    val gradientMagnitude = dataset.gradient.map(math.abs).sum / dataset.gradient.size.toDouble
    val loss = l1 + (gradientMagnitude * 0.05) + (1.0 / dataset.samples.toDouble)
    val accuracy = (1.0 / (1.0 + loss)).min(0.99)
    model -> LocalMetrics(loss, accuracy)

  def fedAvg(updates: List[LocalUpdate], fallback: Model): Model =
    if updates.isEmpty then fallback
    else
      val totalSamples = updates.map(_.samples.toDouble).sum
      val dimensions = fallback.weights.indices
      val aggregated = dimensions.map: index =>
        updates.map(update => update.model.weights(index) * update.samples.toDouble).sum / totalSamples
      Model(aggregated.toVector)

  def pretty(model: Model): String =
    model.weights.map(weight => f"$weight%.3f").mkString("[", ", ", "]")

  def prettyDistance(distance: Double): String =
    if distance >= 5e8 then "inf"
    else f"$distance%.2f"
end NebulaDomain
