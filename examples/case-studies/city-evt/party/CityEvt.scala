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
import java.util.UUID
import io.github.party.handlers.MultitierHandler
import io.github.party.handlers.CollectiveHandler
import io.github.party.signal.Signal.signalBuilder
import io.github.party.signal.Signal
import io.github.party.Multitier.*
import io.github.party.Collective.*
import scala.concurrent.duration.DurationInt
import io.github.party.CollectiveBuildingBlocks.DistanceSensor
import io.github.party.CollectiveBuildingBlocks.DistanceSensor.nbrRange

object CityEvt:
  type Cloud <: { type Tie <: Single[Edge] & Multiple[Smartphone] }
  type Edge <: { type Tie <: Single[Cloud] & Multiple[Smartphone] }
  type Smartphone <: { type Tie <: Single[Cloud] & Single[Edge] & Multiple[Smartphone] }

  type Token = UUID

  final case class RegistrationForm(username: String, email: String)

  final case class Recommendation(content: String)

  private var registeredUsers: Map[String, Token] on Cloud = place(Map.empty)
  private var userToken: Option[Token] on Smartphone = place(None)
  private val devicePositions = Map(
    "smartphone-1" -> (0.0, 0.0),
    "smartphone-2" -> (1.5, 0.0),
  )

  def userRegistration(userData: (String, String) on Smartphone)(using Network, Placement, Multitier) = Multitier:
    val registration = on[Smartphone]:
      signalBuilder[RegistrationForm]: em =>
        Thread.sleep(1000) // Simulate user filling the registration form
        em.emit(RegistrationForm(take(userData)._1, take(userData)._2))
    val registered = on[Cloud]:
      Signal
        .merge(asLocalAll(registration).values.toSeq)
        .map: form =>
          val token = UUID.randomUUID()
          registeredUsers = place(take(registeredUsers) + (form.username -> token))
          form.email -> token
    on[Smartphone]:
      val res = asLocal[Smartphone, Cloud](registered).filter(_._1 == take(userData)._2).first._2
      println(s"User ${peerAddress} with email ${take(userData)._2} has been registered with token: ${res}")
      res

  def recommendation(smartphoneToken: Token on Smartphone)(using Network, Placement, Choreography, Multitier) = Multitier:
    val authRequests = on[Edge] { asLocalAll(smartphoneToken).toMap.map { case (k, v) => k.asInstanceOf[String] -> v } }
    val grantedResults = Choreography:
      val requests = comm[Edge, Cloud](authRequests)
      val authGranted = on[Cloud]:
        take(requests).map:
          case (peerId, token) => peerId -> take(registeredUsers).exists(_._2 == token)
      comm[Cloud, Edge](authGranted)
    val recommendations = on[Edge]:
      val authPeers = take(grantedResults).filter(_._2).keySet
      signalBuilder[(String, Recommendation)]: em =>
        while true do
          Thread.sleep(1000) // Simulate waiting for new recommendations
          authPeers.foreach { peerId =>
            em.emit(peerId -> Recommendation(s"Hello $peerId, here is your recommendation!"))
          }
    on[Smartphone]:
      asLocal[Smartphone, Edge](recommendations).filter(_._1 == peerAddress.asInstanceOf[String]).map(_._2).subscribe { recommendation =>
        println(s"User ${peerAddress} received recommendation: ${recommendation.content}")
      }

  def crowdMonitoring(using Network, Placement, Collective, Multitier) =
    given DistanceSensor = new DistanceSensor:
      val localPeer = peerAddress.asInstanceOf[String]
      def nbrRange(using Collective, VM): Field[Double] =
        val localPos = devicePositions(localPeer)
        nbr(localPos).map { position =>
          math.sqrt(
            math.pow(localPos._1 - position._1, 2) +
              math.pow(localPos._2 - position._2, 2),
          )
        }

    val crowingLevel = Collective[Smartphone](1.seconds):
      rep(0.0): prev =>
        nbrRange.sum / nbrRange.withoutSelf.size
    Multitier:
      on[Cloud]:
        asLocalAll(crowingLevel).values.foreach { signal =>
          signal.subscribe { crowd =>
            if crowd < 1.0 then println(s"Crowd level is low: $crowd")
            else if crowd < 2.0 then println(s"Crowd level is medium: $crowd")
            else println(s"Crowd level is high: $crowd")
          }
        }
  end crowdMonitoring

  def cityEvtApp(using Network, Placement, Multitier, Choreography, Collective) =
    val usrEmail = on[Smartphone]:
      val username = peerAddress.asInstanceOf[String] // Using peer address as username for simplicity
      val email = s"$username@example.com"
      (username, email)
    val smartphoneToken = userRegistration(usrEmail)
    recommendation(smartphoneToken)
    crowdMonitoring
    Thread.sleep(6000) // Keep the program running for a while to observe the output

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, PlacementType, Choreography, Collective, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given ptHandler: PlacementType = PlacementTypeHandler.handler[P]
    given cHandler: Choreography = ChoreographyHandler.handler[P]
    given Collective = CollectiveHandler.handle[P, String, V]
    given Multitier = MultitierHandler.handler[P]
    program

  def main(args: Array[String]): Unit =
    println("Running CityEvt choreography...")
    val broker = InMemoryNetwork.broker()
    val clientNetwork = InMemoryNetwork[Cloud]("cloud", broker)
    val primaryNetwork = InMemoryNetwork[Edge]("edge", broker)
    val smartphone1Network = InMemoryNetwork[Smartphone]("smartphone-1", broker)
    val smartphone2Network = InMemoryNetwork[Smartphone]("smartphone-2", broker)

    val clientFuture = Future { handleProgramForPeer[Cloud](clientNetwork)(cityEvtApp) }
    val primaryFuture = Future { handleProgramForPeer[Edge](primaryNetwork)(cityEvtApp) }
    val smartphone1Future = Future { handleProgramForPeer[Smartphone](smartphone1Network)(cityEvtApp) }
    val smartphone2Future = Future { handleProgramForPeer[Smartphone](smartphone2Network)(cityEvtApp) }

    // Wait for both peers to finish
    val combinedFuture = Future.sequence(Seq(clientFuture, primaryFuture, smartphone1Future, smartphone2Future))
    Await.result(combinedFuture, scala.concurrent.duration.Duration.Inf)
end CityEvt
