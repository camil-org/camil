package io.github.party

import io.github.party.peers.Peers.Cardinality.*
import io.github.party.network.Network
import io.github.party.network.Network.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.Choreography.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.place
import io.github.party.peers.Peers.Peer
import io.github.party.peers.Peers.PeerTag
import io.github.party.raise.Raise
import io.github.party.network.NetworkError
import io.github.party.handlers.PlacementTypeHandler
import io.github.party.handlers.ChoreographyHandler
import io.github.party.distributed.InMemoryNetwork
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await
import io.github.party.Choreography

object BookSeller:
  type Buyer <: { type Tie <: Single[Seller] }
  type Seller <: { type Tie <: Single[Buyer] }

  case class BookOrder(title: String, budget: Double, deliveryAddress: String)
  case class OrderResult(success: Boolean, message: String)

  private val bookCatalog: Map[String, Double] on Seller = place(
    Map(
      "Deep Learning with Python" -> 45.99,
      "Types and Programming Languages" -> 89.99,
      "Structure and Interpretation of Computer Programs" -> 55.00,
      "The Art of Computer Programming" -> 199.99,
      "Introduction to Algorithms" -> 79.99,
    ),
  )

  def bookStoreProtocol(using Network, Choreography, Placement) = Choreography:
    val order = on[Buyer]:
      val bookOrder = BookOrder("Deep Learning with Python", 50.00, "123 Main St")
      println(s"[Buyer ${peerAddress}] Placing order: $bookOrder")
      bookOrder
    val toSeller = comm[Buyer, Seller](order)
    val decision = on[Seller]:
      val orderReceived = take(toSeller)
      val catalog = take(bookCatalog)
      val price = catalog(orderReceived.title)
      if price > 0 && price <= orderReceived.budget then
        val orderId = java.util.UUID.randomUUID().toString.take(8)
        println(s"[Seller ${peerAddress}] Order $orderId accepted! Shipping to ${orderReceived.deliveryAddress}")
        OrderResult(true, s"Order $orderId confirmed! '${orderReceived.title}' ($$${price}) shipping to ${orderReceived.deliveryAddress}")
      else if price > orderReceived.budget then
        println(s"[Seller ${peerAddress}] Order declined - price $$${price} exceeds budget $$${orderReceived.budget}")
        OrderResult(false, s"Order declined: '${orderReceived.title}' costs $$${price}, exceeds budget $$${orderReceived.budget}")
      else
        println(s"[Seller ${peerAddress}] Book not found: '${orderReceived.title}'")
        OrderResult(false, s"Book not found: '${orderReceived.title}'")
    val toBuyer = comm[Seller, Buyer](decision)
    on[Buyer]:
      val result = take(toBuyer)
      if result.success then println(s"[Buyer ${peerAddress}] Order successful: ${result.message}")
      else println(s"[Buyer ${peerAddress}] Order failed: ${result.message}")

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Choreography) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running BookSeller choreography...")
    val broker = InMemoryNetwork.broker()
    val clientNetwork = InMemoryNetwork[Buyer]("buyer", broker)
    val primaryNetwork = InMemoryNetwork[Seller]("seller", broker)

    val clientFuture = Future { handleProgramForPeer[Buyer](clientNetwork)(bookStoreProtocol) }
    val primaryFuture = Future { handleProgramForPeer[Seller](primaryNetwork)(bookStoreProtocol) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(clientFuture, primaryFuture))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end BookSeller
