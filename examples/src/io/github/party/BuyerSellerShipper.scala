package io.github.party

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.ChoreographyHandler
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.network.Network
import io.github.party.network.NetworkError
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object BuyerSellerShipper:
  type Seller <: { type Tie <: Single[Buyer] }
  type Buyer <: { type Tie <: Single[Seller] & Single[Shipper] }
  type Shipper <: { type Tie <: Single[Buyer] }

  final case class Price(amount: Int, currency: String)
  final case class Customer(name: String, address: String)
  final case class Order(title: String, budget: Int, customer: Customer)

  private val catalogue = Map(
    "Types and Programming Languages" -> Price(80, "EUR"),
    "Programming in Scala" -> Price(55, "EUR"),
  )

  def buyerSellerShipperProtocol(using Network, Placement, Choreography) = Choreography:
    val orderAtBuyer = on[Buyer]:
      val order = Order(
        "Types and Programming Languages",
        100,
        Customer("Ada", "123 Main St"),
      )
      println(s"[Buyer] requesting '${order.title}' with budget ${order.budget}")
      order

    val titleAtSeller = comm[Buyer, Seller](on[Buyer] { take(orderAtBuyer).title })
    val quoteAtSeller: Option[Price] on Seller = on[Seller]:
      catalogue.get(take(titleAtSeller))
    val quoteAtBuyer = comm[Seller, Buyer](quoteAtSeller)

    val acceptedAtBuyer: Boolean on Buyer = on[Buyer]:
      val order = take(orderAtBuyer)
      take(quoteAtBuyer).exists(_.amount <= order.budget)
    val accepted = broadcast[Buyer, Boolean](acceptedAtBuyer)

    if accepted then
      val operationAtBuyer: String on Buyer = on[Buyer]:
        val price = take(quoteAtBuyer).get
        s"${price.amount} ${price.currency}"
      val operationAtShipper = comm[Buyer, Shipper](operationAtBuyer)
      val addressAtSeller = comm[Buyer, Seller](on[Buyer] { take(orderAtBuyer).customer.address })

      on[Buyer]:
        val price = take(quoteAtBuyer).get
        println(s"[Buyer] accepted quote ${price.amount} ${price.currency}")
      on[Shipper]:
        println(s"[Shipper] Buyer shipped ${take(operationAtShipper)}")
      on[Seller]:
        println(s"[Seller] shipping '${take(titleAtSeller)}' to ${take(addressAtSeller)}")
    else
      on[Buyer]:
        println("[Buyer] order rejected")

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    val broker = InMemoryNetwork.broker()
    val seller = InMemoryNetwork[Seller]("seller", broker)
    val buyer = InMemoryNetwork[Buyer]("buyer", broker)
    val shipper = InMemoryNetwork[Shipper]("shipper", broker)

    val fs = Future { handleProgramForPeer[Seller](seller)(buyerSellerShipperProtocol) }
    val fb = Future { handleProgramForPeer[Buyer](buyer)(buyerSellerShipperProtocol) }
    val fsh = Future { handleProgramForPeer[Shipper](shipper)(buyerSellerShipperProtocol) }

    Await.result(Future.sequence(Seq(fs, fb, fsh)), duration.Duration.Inf)
end BuyerSellerShipper
