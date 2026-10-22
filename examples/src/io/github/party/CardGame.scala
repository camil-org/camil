package io.github.party

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

/**
 * Simple blackjack-style card game choreography.
 *
 * Ported from a Haskell Choreography/CLI program.
 *
 * Protocol:
 *   1. Dealer deals one card to each player (face up — visible to everyone via broadcast).
 *   2. Each player privately decides whether to request a second card.
 *   3. For each player who asked, dealer sends them a second card (private comm). The deck is consumed sequentially, so the index depends on prior
 *      choices.
 *   4. Dealer reveals a common table card (broadcast to all).
 *   5. Each player individually wins if the sum of their cards (mod 21) > 19.
 */
object CardGame:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // ──────────────────────────────────────────────────────────────────────────

  type Dealer <: { type Tie <: Single[Player1] & Single[Player2] & Single[Player3] }
  type Player1 <: { type Tie <: Single[Dealer] }
  type Player2 <: { type Tie <: Single[Dealer] }
  type Player3 <: { type Tie <: Single[Dealer] }

  // ──────────────────────────────────────────────────────────────────────────
  // Card type — values are always reduced mod 21
  // ──────────────────────────────────────────────────────────────────────────

  case class Card(value: Int) extends Ordered[Card]:
    def +(other: Card): Card = Card(this.value + other.value)
    def compare(that: Card): Int = this.value.compare(that.value)
    override def toString: String = s"Card($value)"

  object Card:
    def apply(n: Int): Card = new Card(((n % 21) + 21) % 21)

  // ──────────────────────────────────────────────────────────────────────────
  // Reference implementation (for verification)
  // ──────────────────────────────────────────────────────────────────────────

  /** Pure reference computation matching the Haskell semantics. */
  def reference(deck: List[Int], choices: (Boolean, Boolean, Boolean)): (Boolean, Boolean, Boolean) =
    val (c1, c2, c3) = choices
    val cycled = LazyList.continually(deck).flatten.map(Card(_))
    val h11 = cycled(0); val h21 = cycled(1); val h31 = cycled(2)
    var idx = 3
    val h12 = if c1 then
      val c = cycled(idx); idx += 1; List(h11, c)
    else List(h11)
    val h22 = if c2 then
      val c = cycled(idx); idx += 1; List(h21, c)
    else List(h21)
    val h32 = if c3 then
      val c = cycled(idx); idx += 1; List(h31, c)
    else List(h31)
    val common = cycled(idx)
    def wins(hand: List[Card]): Boolean = (common :: hand).reduce(_ + _) > Card(19)
    (wins(h12), wins(h22), wins(h32))

  // ──────────────────────────────────────────────────────────────────────────
  // Choreography
  // ──────────────────────────────────────────────────────────────────────────

  def game(
      deck: List[Int],
      choices: (Boolean, Boolean, Boolean),
  )(using Network, Placement, Choreography) = Choreography:
    val (c1, c2, c3) = choices
    val cycledDeck = LazyList.continually(deck).flatten.map(Card(_))

    // ── Step 1: Dealer deals initial cards (face up, broadcast to everyone) ─

    val initCard1: Card on Dealer = on[Dealer]:
      val c = cycledDeck(0)
      println(s"[Dealer] Card for Player1: $c")
      c

    val initCard2: Card on Dealer = on[Dealer]:
      val c = cycledDeck(1)
      println(s"[Dealer] Card for Player2: $c")
      c

    val initCard3: Card on Dealer = on[Dealer]:
      val c = cycledDeck(2)
      println(s"[Dealer] Card for Player3: $c")
      c

    val h1p1: Card = broadcast[Dealer, Card](initCard1)
    val h1p2: Card = broadcast[Dealer, Card](initCard2)
    val h1p3: Card = broadcast[Dealer, Card](initCard3)

    // ── Step 2: Each player decides whether to hit ──────────────────────────

    val wantsP1: Boolean on Player1 = on[Player1]:
      println(s"[Player1] Cards on table: $h1p1, $h1p2, $h1p3")
      println(s"[Player1] Asking for another card? $c1")
      c1

    val wantsP2: Boolean on Player2 = on[Player2]:
      println(s"[Player2] Cards on table: $h1p1, $h1p2, $h1p3")
      println(s"[Player2] Asking for another card? $c2")
      c2

    val wantsP3: Boolean on Player3 = on[Player3]:
      println(s"[Player3] Cards on table: $h1p1, $h1p2, $h1p3")
      println(s"[Player3] Asking for another card? $c3")
      c3

    // ── Step 3: Players communicate choices to dealer; dealer sends 2nd cards
    //    Deck cursor starts at 3 (after the three initial cards).

    var deckIdx = 3

    // Player1
    val choiceP1AtDealer = comm[Player1, Dealer](wantsP1)
    val secondCardP1: Option[Card] on Dealer = on[Dealer]:
      if take(choiceP1AtDealer) then
        val c = cycledDeck(deckIdx); deckIdx += 1
        println(s"[Dealer] Second card for Player1: $c")
        Some(c)
      else None
    val secondP1 = comm[Dealer, Player1](secondCardP1)

    // Player2
    val choiceP2AtDealer = comm[Player2, Dealer](wantsP2)
    val secondCardP2: Option[Card] on Dealer = on[Dealer]:
      if take(choiceP2AtDealer) then
        val c = cycledDeck(deckIdx); deckIdx += 1
        println(s"[Dealer] Second card for Player2: $c")
        Some(c)
      else None
    val secondP2 = comm[Dealer, Player2](secondCardP2)

    // Player3
    val choiceP3AtDealer = comm[Player3, Dealer](wantsP3)
    val secondCardP3: Option[Card] on Dealer = on[Dealer]:
      if take(choiceP3AtDealer) then
        val c = cycledDeck(deckIdx); deckIdx += 1
        println(s"[Dealer] Second card for Player3: $c")
        Some(c)
      else None
    val secondP3 = comm[Dealer, Player3](secondCardP3)

    // ── Step 4: Dealer reveals common table card ────────────────────────────

    val tableCardOnDealer: Card on Dealer = on[Dealer]:
      val c = cycledDeck(deckIdx)
      println(s"[Dealer] Table card: $c")
      c

    val tableCard: Card = broadcast[Dealer, Card](tableCardOnDealer)

    // ── Step 5: Each player determines if they won ──────────────────────────

    on[Player1]:
      val hand = take(secondP1) match
        case Some(c2) => List(h1p1, c2)
        case None => List(h1p1)
      val total = (tableCard :: hand).reduce(_ + _)
      val win = total > Card(19)
      println(s"[Player1] Hand: $hand, table: $tableCard, total: $total, win: $win")

    on[Player2]:
      val hand = take(secondP2) match
        case Some(c2) => List(h1p2, c2)
        case None => List(h1p2)
      val total = (tableCard :: hand).reduce(_ + _)
      val win = total > Card(19)
      println(s"[Player2] Hand: $hand, table: $tableCard, total: $total, win: $win")

    on[Player3]:
      val hand = take(secondP3) match
        case Some(c2) => List(h1p3, c2)
        case None => List(h1p3)
      val total = (tableCard :: hand).reduce(_ + _)
      val win = total > Card(19)
      println(s"[Player3] Hand: $hand, table: $tableCard, total: $total, win: $win")

  // ──────────────────────────────────────────────────────────────────────────
  // Peer runner
  // ──────────────────────────────────────────────────────────────────────────

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running CardGame choreography...")

    // Example deck and choices (P1 and P3 ask for a second card, P2 does not)
    val deck = List(7, 3, 15, 10, 8, 12, 5)
    val choices = (true, false, true)

    // Verify reference computation
    val (r1, r2, r3) = reference(deck, choices)
    println(s"Reference results: Player1=$r1, Player2=$r2, Player3=$r3")

    val broker = InMemoryNetwork.broker()
    val dealerNet = InMemoryNetwork[Dealer]("dealer", broker)
    val player1Net = InMemoryNetwork[Player1]("player1", broker)
    val player2Net = InMemoryNetwork[Player2]("player2", broker)
    val player3Net = InMemoryNetwork[Player3]("player3", broker)

    val f1 = Future { handleProgramForPeer[Dealer](dealerNet)(game(deck, choices)) }
    val f2 = Future { handleProgramForPeer[Player1](player1Net)(game(deck, choices)) }
    val f3 = Future { handleProgramForPeer[Player2](player2Net)(game(deck, choices)) }
    val f4 = Future { handleProgramForPeer[Player3](player3Net)(game(deck, choices)) }

    Await.result(
      Future.sequence(Seq(f1, f2, f3, f4)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("CardGame done.")
  end main
end CardGame
