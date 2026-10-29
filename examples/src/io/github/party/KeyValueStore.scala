package io.github.party

import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.{ broadcast, comm, multicast }
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.{ ChoreographyHandler, PlacementTypeHandler }
import io.github.party.network.{ Network, NetworkError }
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.{ Peer, PeerTag }
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.*
import io.github.party.raise.Raise

object KeyValueStore:
  type Client <: { type Tie <: Single[Primary] }
  type Primary <: { type Tie <: Single[Client] & Multiple[Backup] }
  type Backup <: { type Tie <: Multiple[Backup] & Single[Primary] }

  private val store: mutable.Map[String, String] = mutable.Map.empty

  // Request ADT for client operations
  enum Request:
    case Get(key: String)
    case Put(key: String, value: String)

  // Response ADT
  type Response = Option[String]

  // Acknowledgment from backup servers
  final case class BackupAck(backupId: String, success: Boolean)

  private def handleRequest(request: Request): Response = request match
    case Request.Get(key) => store.get(key)
    case Request.Put(key, value) => store += (key -> value); None

  def kvStoreProtocol(request: Request on Client)(using Network, Placement, Choreography): Response on Client = Choreography:
    val requestOnPrimary = comm[Client, Primary](request)
    val req = broadcast(requestOnPrimary)
    req match
      case Request.Put(key, value) =>
        val requestOnBackup = multicast[Primary, Backup](requestOnPrimary)
        on[Backup]:
          println(s"[Backup $peerAddress] Received update request: $key -> $value")
          handleRequest(take(requestOnBackup))
      case Request.Get(key) => ()
    val response = on[Primary]:
      println(s"[Primary $peerAddress] Handling request: $req")
      handleRequest(take(requestOnPrimary))
    comm[Primary, Client](response)

  def kvs(using Network, Placement, Choreography) = Choreography:
    val operations =
      List(Request.Put("foo", "bar"), Request.Get("foo"), Request.Put("hello", "world"), Request.Get("hello"), Request.Get("nonexistent"))
    operations.foreach: op =>
      val request = on[Client](op)
      val response = kvStoreProtocol(request)
      on[Client] { println(s"[Client ${peerAddress}] Received response: ${take(response)} for request: ${take(request)}") }

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Choreography) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running KeyValueStore choreography...")
    val broker = InMemoryNetwork.broker()
    val clientNetwork = InMemoryNetwork[Client]("client", broker)
    val primaryNetwork = InMemoryNetwork[Primary]("primary", broker)
    val backup1Network = InMemoryNetwork[Backup]("backup1", broker)
    val backup2Network = InMemoryNetwork[Backup]("backup2", broker)

    val clientFuture = Future { handleProgramForPeer[Client](clientNetwork)(kvs) }
    val primaryFuture = Future { handleProgramForPeer[Primary](primaryNetwork)(kvs) }
    val backup1Future = Future { handleProgramForPeer[Backup](backup1Network)(kvs) }
    val backup2Future = Future { handleProgramForPeer[Backup](backup2Network)(kvs) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(clientFuture, primaryFuture, backup1Future, backup2Future))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end KeyValueStore
