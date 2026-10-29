package io.github.party

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ MultitierHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.*
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise

object TicketTrader:
  type Buyer <: { type Tie <: Multiple[Vendor] }
  type Vendor <: { type Tie <: Single[Buyer] }

  final case class Coordinate(x: Double, y: Double)
  final case class TicketOffer(eventName: String, price: Double, location: Coordinate, vendorId: String)
  final case class BuyerPreference(eventName: String, maxPrice: Double, maxDistance: Double)

  private val defaultPreference = BuyerPreference("Rock Werchter", 200.0, 500.0)
  private val walkSteps = 6
  private val walkStepSize = 80.0

  private val initialPositions = Map(
    "buyer" -> Coordinate(0.0, 0.0),
    "vendor-1" -> Coordinate(120.0, 90.0),
    "vendor-2" -> Coordinate(900.0, 900.0),
  )

  private val seedsByPeer = Map(
    "buyer" -> 11L,
    "vendor-1" -> 22L,
    "vendor-2" -> 33L,
  )

  private def randomWalk(start: Coordinate, steps: Int, stepSize: Double, seed: Long): List[Coordinate] =
    val rng = scala.util.Random(seed)
    (1 to steps)
      .foldLeft(List(start)): (path, _) =>
        val current = path.head
        val angle = rng.nextDouble() * (2.0 * math.Pi)
        val delta = rng.nextDouble() * stepSize
        val next = Coordinate(
          current.x + math.cos(angle) * delta,
          current.y + math.sin(angle) * delta,
        )
        next :: path
      .reverse

  private def distance(a: Coordinate, b: Coordinate): Double =
    math.hypot(a.x - b.x, a.y - b.y)

  private def isMatch(offer: TicketOffer, pref: BuyerPreference, buyerLocation: Coordinate): Boolean =
    offer.eventName == pref.eventName &&
      offer.price <= pref.maxPrice &&
      distance(offer.location, buyerLocation) <= pref.maxDistance

  private def initialPosition(peerId: String): Coordinate =
    initialPositions.getOrElse(peerId, Coordinate(0.0, 0.0))

  private def seed(peerId: String): Long =
    seedsByPeer.getOrElse(peerId, math.abs(peerId.hashCode.toLong) + 41L)

  private def fmtCoordinate(c: Coordinate): String =
    f"(${c.x}%.2f, ${c.y}%.2f)"

  private def fmtPath(path: List[Coordinate]): String =
    path.map(fmtCoordinate).mkString(" -> ")

  private def buildOffer(vendorId: String, snapshotLocation: Coordinate): TicketOffer =
    vendorId match
      case "vendor-1" => TicketOffer("Rock Werchter", 140.0, snapshotLocation, vendorId)
      case _ => TicketOffer("Jazz Fest", 350.0, snapshotLocation, vendorId)

  def ticketTraderProtocol(using Network, Placement, Multitier) = Multitier:
    val buyerSnapshot = on[Buyer]:
      val buyerId = peerAddress.asInstanceOf[String]
      val path = randomWalk(initialPosition(buyerId), walkSteps, walkStepSize, seed(buyerId))
      val snapshot = path.last
      println(s"[Buyer $buyerId] Random walk path: ${fmtPath(path)}")
      println(s"[Buyer $buyerId] Snapshot location: ${fmtCoordinate(snapshot)}")
      println(
        s"[Buyer $buyerId] Preferences: event='${defaultPreference.eventName}', maxPrice=${defaultPreference.maxPrice}, maxDistance=${defaultPreference.maxDistance}",
      )
      (defaultPreference, snapshot)

    val vendorOffer = on[Vendor]:
      val vendorId = peerAddress.asInstanceOf[String]
      val path = randomWalk(initialPosition(vendorId), walkSteps, walkStepSize, seed(vendorId))
      val snapshot = path.last
      val offer = buildOffer(vendorId, snapshot)
      println(s"[Vendor $vendorId] Random walk path: ${fmtPath(path)}")
      println(
        s"[Vendor $vendorId] Published offer: event='${offer.eventName}', price=${offer.price}, location=${fmtCoordinate(offer.location)}",
      )
      offer

    on[Buyer]:
      val (preference, buyerLocation) = take(buyerSnapshot)
      val allOffers = asLocalAll[Buyer, Vendor](vendorOffer).values.toList.sortBy(_.vendorId)
      println(s"[Buyer $peerAddress] Collected ${allOffers.size} offers.")
      allOffers.foreach: offer =>
        val offerDistance = distance(buyerLocation, offer.location)
        println(
          s"[Buyer $peerAddress] Offer from ${offer.vendorId}: event='${offer.eventName}', price=${offer.price}, distance=${f"$offerDistance%.2f"}",
        )
      val matches = allOffers.filter(isMatch(_, preference, buyerLocation))
      println(s"[Buyer $peerAddress] Matched ${matches.size} offers.")
      if matches.isEmpty then println(s"[Buyer $peerAddress] No offer satisfies all filters.")
      else
        matches.foreach: offer =>
          println(
            s"[Buyer $peerAddress] MATCH -> vendor=${offer.vendorId}, event='${offer.eventName}', price=${offer.price}, location=${fmtCoordinate(offer.location)}",
          )

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Multitier) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running TicketTrader multitier snapshot...")
    val broker = InMemoryNetwork.broker()
    val buyerNetwork = InMemoryNetwork[Buyer]("buyer", broker)
    val vendor1Network = InMemoryNetwork[Vendor]("vendor-1", broker)
    val vendor2Network = InMemoryNetwork[Vendor]("vendor-2", broker)

    val buyerFuture = Future { handleProgramForPeer[Buyer](buyerNetwork)(ticketTraderProtocol) }
    val vendor1Future = Future { handleProgramForPeer[Vendor](vendor1Network)(ticketTraderProtocol) }
    val vendor2Future = Future { handleProgramForPeer[Vendor](vendor2Network)(ticketTraderProtocol) }

    val combinedFuture = Future.sequence(Seq(buyerFuture, vendor1Future, vendor2Future))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
    println("TicketTrader done.")
end TicketTrader
