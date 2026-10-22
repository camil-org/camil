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
import io.github.party.placement.PlacementType
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

/**
 * Choreographic Tic-Tac-Toe game.
 *
 * Ported from the ChoRus (Rust) tic-tac-toe example.
 *
 * Two players (PlayerX and PlayerO) take turns. Each player has a "brain" that decides the next move. The board state is broadcast after every turn
 * so both peers stay in sync. The game ends when a player wins or the board is full (tie).
 *
 * For the automated demo both players use a minimax AI brain.
 */
object TicTacToe:

  // ──────────────────────────────────────────────────────────────────────────
  // Peer definitions
  // ──────────────────────────────────────────────────────────────────────────

  type PlayerX <: { type Tie <: Single[PlayerO] }
  type PlayerO <: { type Tie <: Single[PlayerX] }

  // ──────────────────────────────────────────────────────────────────────────
  // Game status
  // ──────────────────────────────────────────────────────────────────────────

  enum Status:
    case InProgress
    case PlayerWon(player: Char)
    case Tie

  // ──────────────────────────────────────────────────────────────────────────
  // Board
  // ──────────────────────────────────────────────────────────────────────────

  case class Board(cells: Array[Char]):
    def draw(): Unit =
      for i <- (0 until 3).reverse do
        val offset = i * 3
        println("-------------")
        println(s"| ${cells(offset)} | ${cells(offset + 1)} | ${cells(offset + 2)} |")
      println("-------------")

    def check: Status =
      import scala.util.boundary, boundary.break
      boundary:
        // Check rows
        for i <- 0 until 3 do
          val o = i * 3
          if cells(o) == cells(o + 1) && cells(o + 1) == cells(o + 2) then break(Status.PlayerWon(cells(o)))
        // Check columns
        for i <- 0 until 3 do if cells(i) == cells(i + 3) && cells(i + 3) == cells(i + 6) then break(Status.PlayerWon(cells(i)))
        // Check diagonals
        if cells(0) == cells(4) && cells(4) == cells(8) then break(Status.PlayerWon(cells(0)))
        if cells(2) == cells(4) && cells(4) == cells(6) then break(Status.PlayerWon(cells(2)))
        // Check for tie (no empty cells)
        for i <- 0 until 9 do if cells(i) == Character.forDigit(i, 10) then break(Status.InProgress)
        Status.Tie

    def mark(player: Char, pos: Int): Board =
      val newCells = cells.clone()
      newCells(pos) = player
      Board(newCells)
  end Board

  object Board:
    def empty: Board = Board(Array.tabulate(9)(i => Character.forDigit(i, 10)))

  // ──────────────────────────────────────────────────────────────────────────
  // Brain trait — decides the next move
  // ──────────────────────────────────────────────────────────────────────────

  trait Brain:
    def think(board: Board): Board

  /** Minimax AI brain — always plays optimally. */
  class MinimaxBrain(player: Char) extends Brain:
    private def minimax(board: Board, current: Char): (Int, Int) =
      board.check match
        case Status.PlayerWon(p) => if p == player then (1, 0) else (-1, 0)
        case Status.Tie => (0, 0)
        case Status.InProgress =>
          var bestScore = if current == player then Int.MinValue else Int.MaxValue
          var bestMove = 0
          for i <- 0 until 9 do
            if board.cells(i) == Character.forDigit(i, 10) then
              val newBoard = board.mark(current, i)
              val next = if current == 'X' then 'O' else 'X'
              val (score, _) = minimax(newBoard, next)
              if current == player then
                if score > bestScore then
                  bestScore = score; bestMove = i
              else if score < bestScore then
                bestScore = score; bestMove = i
          (bestScore, bestMove)

    def think(board: Board): Board =
      board.draw()
      println(s"Player $player: Thinking...")
      val (_, bestMove) = minimax(board, player)
      val newBoard = board.mark(player, bestMove)
      println(s"$player: Marked position $bestMove")
      newBoard.draw()
      newBoard
  end MinimaxBrain

  // ──────────────────────────────────────────────────────────────────────────
  // Choreography
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Recursive game loop.
   *
   * Each iteration: PlayerX moves → broadcast → check. If still in progress: PlayerO moves → broadcast → check → recurse.
   */
  def gameLoop(
      board: Board,
      brainX: Brain,
      brainO: Brain,
  )(using Network, Placement, Choreography): Board = Choreography:
    // PlayerX's turn
    val boardAfterX: Board on PlayerX = on[PlayerX]:
      brainX.think(board)
    val boardX: Board = broadcast[PlayerX, Board](boardAfterX)
    boardX.check match
      case Status.InProgress => // continue to PlayerO's turn
        val boardAfterO: Board on PlayerO = on[PlayerO]:
          brainO.think(boardX)
        val boardO: Board = broadcast[PlayerO, Board](boardAfterO)
        boardO.check match
          case Status.InProgress => gameLoop(boardO, brainX, brainO)
          case _ => boardO
      case _ => boardX

  def ticTacToeProtocol(
      brainX: Brain,
      brainO: Brain,
  )(using Network, Placement, Choreography) = Choreography:
    val finalBoard = gameLoop(Board.empty, brainX, brainO)
    finalBoard.check match
      case Status.PlayerWon('X') =>
        on[PlayerX]:
          println("You win!")
        on[PlayerO]:
          println("You lose")
      case Status.PlayerWon('O') =>
        on[PlayerX]:
          println("You lose")
        on[PlayerO]:
          println("You win!")
      case Status.Tie =>
        println("Tie!")
      case _ => ()

  // ──────────────────────────────────────────────────────────────────────────
  // Peer runner
  // ──────────────────────────────────────────────────────────────────────────

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, PlacementType, Choreography) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given ptHandler: PlacementType = PlacementTypeHandler.handler[P]
    given cHandler: Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running Tic-Tac-Toe choreography (Minimax vs Minimax)...")
    val broker = InMemoryNetwork.broker()
    val playerXNet = InMemoryNetwork[PlayerX]("playerX", broker)
    val playerONet = InMemoryNetwork[PlayerO]("playerO", broker)

    val brainX = MinimaxBrain('X')
    val brainO = MinimaxBrain('O')

    val f1 = Future { handleProgramForPeer[PlayerX](playerXNet)(ticTacToeProtocol(brainX, brainO)) }
    val f2 = Future { handleProgramForPeer[PlayerO](playerONet)(ticTacToeProtocol(brainX, brainO)) }

    Await.result(
      Future.sequence(Seq(f1, f2)),
      scala.concurrent.duration.Duration.Inf,
    )
    println("Tic-Tac-Toe done.")
end TicTacToe
