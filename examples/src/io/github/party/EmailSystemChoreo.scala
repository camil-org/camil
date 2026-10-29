package io.github.party

import scala.concurrent.*
import scala.concurrent.ExecutionContext.Implicits.global

import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.EmailSystemUtils.*
import io.github.party.distributed.InMemoryNetwork
import io.github.party.handlers.*
import io.github.party.network.{ Network, NetworkError }
import io.github.party.peers.Peers.*
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.PlacementType.on
import io.github.party.raise.Raise

object EmailSystemChoreo:
  type Client <: { type Tie <: Single[Server] }
  type Server <: { type Tie <: Single[Client] }

  def emailSyncProtocol(config: ClientConfig)(using Network, Placement, Choreography) = Choreography:
    val emailsOnServer = on[Server]:
      val ts = 0L
      println(s"[Server] Starting email sync protocol with timestamp $ts")
      createServerDB().since(config.userId, ts)
    val emailsOnClient = comm[Server, Client](emailsOnServer)
    on[Client]:
      val emails = take(emailsOnClient)
      println(s"[Client] Received ${emails.size} emails from server")
      val db = createClientDB()
      db.update(emails)
    val attachmentsOnServer = on[Server]:
      if config.isOnFlatRate then
        val emailIds = take(emailsOnServer).map(_.id)
        println(s"[Server] Fetching attachments for ${emailIds.size} emails (client on flat rate)")
        emailIds.flatMap(createServerDB().getAttachments)
      else
        println("[Server] Skipping attachments (client not on flat rate)")
        List.empty[Attachment]
    val attachmentsOnClient = comm[Server, Client](attachmentsOnServer)
    on[Client]:
      val attachments = take(attachmentsOnClient)
      if attachments.nonEmpty then createClientDB().updateAttachments(attachments)
      println("[Client] Email synchronization completed")
  end emailSyncProtocol

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](program: (Network, Placement, Choreography) ?=> V): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running Email System choreography...")
    val userId = if args.length > 0 then args(0) else "user1"
    val isOnFlatRate = if args.length > 1 then args(1).toBoolean else true
    val config = ClientConfig(userId, isOnFlatRate)

    val broker = InMemoryNetwork.broker()
    val pingerNetwork = InMemoryNetwork[Client]("client", broker)
    val pongerNetwork = InMemoryNetwork[Server]("server", broker)

    val pingerFuture = Future { handleProgramForPeer[Client](pingerNetwork)(emailSyncProtocol(config)) }
    val pongerFuture = Future { handleProgramForPeer[Server](pongerNetwork)(emailSyncProtocol(config)) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(pingerFuture, pongerFuture))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end EmailSystemChoreo
