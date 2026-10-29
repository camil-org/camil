package io.github.party

import io.github.party.peers.Peers.Cardinality.*
import io.github.party.network.Network
import io.github.party.network.Network.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on
import io.github.party.Choreography.*
import io.github.party.placement.PeerScope.take
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
import io.github.party.Choreography
import scala.caps.Stateful
import scala.caps.SharedCapability
import io.github.party.placement.PeerScope
import io.github.party.signal.Signal.merge

object CityEvt:
  type Cloud <: { type Tie <: Single[Edge] & Multiple[Smartphone] }
  type Edge <: { type Tie <: Single[Cloud] & Multiple[Smartphone] }
  type Smartphone <: { type Tie <: Single[Cloud] & Single[Edge] & Multiple[Smartphone] }

  type Token = UUID

  final case class RegistrationForm(username: String, email: String)

  final case class Recommendation(content: String)

  final case class User(username: String, email: String)

  class CityEvtState extends Stateful, SharedCapability:
    private var _registeredUsers: Map[String, Token] = Map.empty

    def registeredUsers(using PeerScope[Cloud]): Map[String, Token] = _registeredUsers

    update def register(username: String)(using PeerScope[Cloud]) =
      val token = UUID.randomUUID()
      _registeredUsers = _registeredUsers + (username -> token)
      token

    def isRegistered(token: Token)(using PeerScope[Cloud]) = _registeredUsers.exists(_._2 == token)

  private val devicePositions = Map(
    "smartphone-1" -> (0.0, 0.0),
    "smartphone-2" -> (1.5, 0.0),
  )

  def userRegistration(cityEvt: CityEvtState)(using Network, Placement, Multitier) = 
    Multitier:
      val user = on[Smartphone]:
        val username = peerAddress.asInstanceOf[String] // Using peer address as username for simplicity
        val email = s"$username@example.com"
        User(username, email)
      val registration = on[Smartphone]:
        signalBuilder[RegistrationForm]: em =>
          Thread.sleep(1000) // Simulate user filling the registration form
          em.emit(RegistrationForm(take(user).username, take(user).email))
      val registered = on[Cloud]:
        signalBuilder[(String, Token)]: em =>
          Signal.merge(asLocalAll(registration).values)
            .map: form =>
              val token = UUID.randomUUID()
              cityEvt.register(form.username)
              em.emit(form.email -> token)
      on[Smartphone]:
        val token = asLocal[Smartphone, Cloud](registered).filter(_._1 == take(user).email).first._2
        println(s"User ${peerAddress} with email ${take(user).email} has been registered with token: ${token}")
        token

  def recommendation(cityEvt: CityEvtState, smartphoneToken: Token on Smartphone)(using Network, Placement, Choreography, Multitier): Unit =
    Multitier:
      val authRequests = on[Edge]:
        asLocalAll(smartphoneToken).toMap.map((k, v) => k.asInstanceOf[String] -> v)
      val grantedResults = Choreography:
        val requests = comm[Edge, Cloud](authRequests)
        val authGranted = on[Cloud]:
          take(requests)
            .map((peerId, token) => peerId -> cityEvt.isRegistered(token))
        comm[Cloud, Edge](authGranted)
      val recommendations = on[Edge]:
        val authPeers = take(grantedResults).filter(_._2).keySet
        signalBuilder[(String, Recommendation)]: em =>
          while true do
            Thread.sleep(1000) // Simulate waiting for new recommendations
            authPeers.foreach: peerId =>
              em.emit(peerId -> Recommendation(s"Hello $peerId, here is your recommendation!"))
      on[Smartphone]:
        asLocal[Smartphone, Edge](recommendations)
          .filter(_._1 == peerAddress.asInstanceOf[String])
          .map(_._2).subscribe: recommendation =>
            println(s"User ${peerAddress} received recommendation: ${recommendation.content}")

  def crowdMonitoring(using Network, Placement, Collective, Multitier) =
    given DistanceSensor = NbrSensor.sensor(devicePositions)

    val crowingLevel = Collective[Smartphone](1.seconds):
      rep(0.0): prev =>
        nbrRange.sum / nbrRange.withoutSelf.size
    Multitier:
      on[Cloud]:
        asLocalAll(crowingLevel).values.foreach: signal =>
          signal.subscribe: crowd =>
            if crowd < 1.0 then println(s"Crowd level is low: $crowd")
            else if crowd < 2.0 then println(s"Crowd level is medium: $crowd")
            else println(s"Crowd level is high: $crowd")

  def cityEvtApp(using Network, Placement, Multitier, Choreography, Collective) =
    val cloudState = CityEvtState()
    val smartphoneToken = userRegistration(cloudState)
    recommendation(cloudState, smartphoneToken)
    crowdMonitoring
    Thread.sleep(6000) // Keep the program running for a while to observe the output

  private def handleProgramForPeer[P <: Peer: PeerTag](net: Network)[V](
      program: (Network, Placement, Choreography, Collective, Multitier) ?=> V,
  ): V =
    given Network = net
    given Raise[NetworkError] = Raise.rethrowError
    given Placement = PlacementTypeHandler.handler[P]
    given Choreography = ChoreographyHandler.handler[P]
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
