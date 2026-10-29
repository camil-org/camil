// package io.github.party

// import java.lang.Thread.sleep
// import java.util.concurrent.atomic.AtomicInteger

// import scala.annotation.tailrec
// import scala.caps.{ any, SharedCapability }
// import scala.concurrent.{ Await, Future }
// import scala.concurrent.ExecutionContext.Implicits.global
// import scala.concurrent.duration.*

// import io.github.party.Choreography.*
// import io.github.party.distributed.InMemoryNetwork
// import io.github.party.handlers.{ ChoreographyHandler, PlacementTypeHandler }
// import io.github.party.network.{ Identifier, Network, NetworkError }
// import io.github.party.peers.Peers.{ Peer, PeerTag, TiedWith }
// import io.github.party.peers.Peers.Cardinality.Single
// import io.github.party.placement.PeerScope.take
// import io.github.party.placement.Placement
// import io.github.party.placement.PlacementType.on
// import io.github.party.raise.Raise
// import scala.jdk.javaapi.DurationConverters

// object NetworkFailurePolicies:

//   final class RetryPolicy(maxAttempts: Int, delay: FiniteDuration):
//     require(maxAttempts > 0, "maxAttempts must be positive")
//     def untilNextAttempt: java.time.Duration = DurationConverters.toJava(delay)
//     def shouldRetry(error: NetworkError, attempt: Int): Boolean =
//       attempt < maxAttempts && (
//         error match
//           case NetworkError.UnreachablePeer(_) => true
//           case NetworkError.NetworkFailure(_) => true
//           case _ => false
//       )

//   final class RetryingNetwork(val underlying: Network, config: RetryPolicy) extends Network:
//     export underlying.{ broadcast as _, pull as _, pullFromAll as _, push as _, retrieve as _, retrieveNow as _, * }

//     private def handle[A](op: Raise[NetworkError] ?=> A)(using default: Raise[NetworkError]): A =
//       @tailrec def attempt(tentative: Int): A =
//         Raise.handleWith(op): err =>
//           if config.shouldRetry(err, tentative) then
//             sleep(config.untilNextAttempt)
//             attempt(tentative + 1)
//           else default.raise(err)
//       attempt(1)

//     override def push[S <: TiedWith[D], D <: Peer, V](using
//         Raise[NetworkError],
//     )(
//         to: PeerAddress,
//         key: Identifier,
//         value: V,
//     ): Unit = handle(underlying.push[S, D, V](to, key, value))

//     override def pull[S <: TiedWith[D], D <: Peer, V](using
//         Raise[NetworkError],
//     )(
//         from: PeerAddress,
//         key: Identifier,
//     ): V = handle(underlying.pull[S, D, V](from, key))

//     override def pullFromAll[S <: TiedWith[D], D <: Peer, V](using
//         Raise[NetworkError],
//     )(
//         from: Set[PeerAddress],
//         key: Identifier,
//     ): Map[PeerAddress, V] = handle(underlying.pullFromAll[S, D, V](from, key))

//     override def broadcast[S <: Peer, V](using Raise[NetworkError])(key: Identifier, value: V): Unit =
//       handle(underlying.broadcast[S, V](key, value))

//     override def retrieve[S <: Peer, V](using Raise[NetworkError])(key: Identifier): V =
//       handle(underlying.retrieve[S, V](key))

//     override def retrieveNow[S <: Peer, V](using Raise[NetworkError])(from: PeerAddress, key: Identifier): Option[V] =
//       handle(underlying.retrieveNow[S, V](from, key))
//   end RetryingNetwork

//   /** Fails before writing, without changing peer discovery or the choreography. */
//   final class FailFirstPush(val underlying: Network, failures: Int = 1) extends Network:
//     require(failures >= 0, "failures must be non-negative")
//     private val attempts = new AtomicInteger(0)

//     def pushAttempts: Int = attempts.get()

//     export underlying.{ push as _, * }

//     override def push[S <: TiedWith[D], D <: Peer, V](using
//         Raise[NetworkError],
//     )(
//         to: PeerAddress,
//         key: Identifier,
//         value: V,
//     ): Unit =
//       val attempt = attempts.incrementAndGet()
//       Raise.ensure(attempt > failures)(NetworkError.UnreachablePeer("simulated transient outage before send"))
//       underlying.push[S, D, V](to, key, value)

//   end FailFirstPush

//   type Pinger <: { type Tie <: Single[Ponger] }
//   type Ponger <: { type Tie <: Single[Pinger] }

//   def program(onEvent: String => Unit)(using Network, Placement, Choreography): Unit = Choreography:
//     val ping = on[Pinger]:
//       onEvent("Pinger preparing ping")
//       "ping"
//     val pingOnPonger = comm[Pinger, Ponger](ping)
//     val pong = on[Ponger]:
//       onEvent(s"Ponger received: ${take(pingOnPonger)}")
//       "pong"
//     val pongOnPinger = comm[Ponger, Pinger](pong)
//     on[Pinger]:
//       onEvent(s"Pinger received: ${take(pongOnPinger)}")
//     ()

//   def runPeer[P <: Peer: PeerTag](network: Network)(
//       program: (Network, Placement, Choreography) ?=> Unit,
//   )(using Raise[NetworkError]): Unit =
//     given Network = network
//     given Placement = PlacementTypeHandler.handler[P]
//     given Choreography = ChoreographyHandler.handler[P]
//     program

//   def runPingPong(pinger: Network, ponger: Network)(
//       onEvent: String => Unit,
//   )(using Raise[NetworkError]): Unit =
//     val ping = Future { runPeer[Pinger](pinger)(program(onEvent)) }
//     val pong = Future { runPeer[Ponger](ponger)(program(onEvent)) }
//     Await.result(ping.zip(pong), 10.seconds)
//     ()

//   def main(args: Array[String]): Unit =
//     val broker = InMemoryNetwork.broker()

//     val policy = RetryPolicy(maxAttempts = 3, delay = 50.milliseconds)
//     val pingTransport = FailFirstPush(InMemoryNetwork[Pinger]("pinger", broker))
//     val pongTransport = FailFirstPush(InMemoryNetwork[Ponger]("ponger", broker))
//     val pingNetwork = RetryingNetwork(pingTransport, policy)
//     val pongNetwork = RetryingNetwork(pongTransport, policy)

//     runPingPong(pingNetwork, pongNetwork)(println)(using Raise.rethrowError)
//     println(s"Send attempts: ping=${pingTransport.pushAttempts}, pong=${pongTransport.pushAttempts}")
//   end main
// end NetworkFailurePolicies
