<div align="center">

# Camil

**A capability-based Scala framework for typed multiparty distributed programming.**

[![CI/CD](https://github.com/camil-org/camil/actions/workflows/dispatcher.yml/badge.svg)](https://github.com/camil-org/camil/actions/workflows/dispatcher.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Scala 3.8.1](https://img.shields.io/badge/Scala-3.8.1-red.svg)](https://www.scala-lang.org)

</div>

Camil lets you express *where* code runs, *which* peers may communicate, and *how* values
move across a distributed topology — all in a single Scala DSL. The repository ships the
core abstractions, an in-memory runtime for local simulation, a broad example suite, and a
Nebula-inspired demo that combines the supported paradigms.

## Features

- **Typed peers & topology** — peer roles and constraints via `Single[...]` and `Multiple[...]`.
- **Choreography** — explicit communication flows: `comm`, `broadcast`, `multicast`, `gather`.
- **Multitier placement** — colocate computation with data using `on`, `asLocal`, `asLocalAll`.
- **Collective programming** — neighborhood-based primitives: `rep`, `nbr`, `branch`, `mux`.
- **Reactive values** — distributed state propagation through `Signal`.
- **In-memory runtime** — a `Network` implementation for runnable local experiments and tests.

## Getting Started

Camil builds with [Mill](https://mill-build.org) on Scala 3.8.1 (experimental capabilities enabled).
A JDK 21+ is required.

```bash
# Run the test suite
./mill core.test

# Run an example
./mill examples.runMain io.github.party.PingPong

# Run the hybrid Nebula demo
./mill nebula.runMain io.github.party.nebula.Nebula
```

> A reproducible dev shell is provided via Nix: run `nix develop` (or use `direnv`) to get
> Mill, a JDK, and the native toolchain.

## Project Layout

| Module        | Contents                                                                      |
| ------------- | ----------------------------------------------------------------------------- |
| `core`        | Peer typing, placements, choreography, multitier, collective, and signals.    |
| `distributed` | Runtime support, including the in-memory network used by the examples.        |
| `examples`    | Runnable examples and case studies built on the core DSL.                     |
| `nebula`      | A compact federated-learning-style demo combining all three paradigms.        |

## The Three Paradigms

Camil exposes three complementary styles of distributed programming. Pick the one that fits
how you want to reason about communication.

<details>
<summary><b>Choreography</b> — communication described directly in program order</summary>

```scala
import io.github.party.Choreography
import io.github.party.Choreography.*
import io.github.party.network.Network
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on

object PingPong:
  type Pinger <: { type Tie <: Single[Ponger] }
  type Ponger <: { type Tie <: Single[Pinger] }

  def program(using Network, Placement, Choreography) = Choreography:
    val pingOnPinger = on[Pinger]:
      "ping"

    val pingOnPonger = comm[Pinger, Ponger](pingOnPinger)

    val pongOnPonger = on[Ponger]:
      s"${take(pingOnPonger)} -> pong"

    val pongOnPinger = comm[Ponger, Pinger](pongOnPonger)

    on[Pinger]:
      println(take(pongOnPinger))
```

Best when the communication pattern is the main concern and you want it spelled out
step by step.

</details>

<details>
<summary><b>Multitier</b> — computation colocated with data, reads across placements stay explicit</summary>

```scala
import io.github.party.Multitier
import io.github.party.Multitier.*
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.network.Network.reachablePeersOf
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on

object MasterWorker:
  type Master <: { type Tie <: Multiple[Worker] }
  type Worker <: { type Tie <: Single[Master] }

  final class Task(input: Int):
    def exec(): Int = input * 2

  def allocateTasks(using n: Network): Map[n.PeerAddress, List[Task]] =
    val inputs = List(1, 2, 3, 4, 5)
    val workers = reachablePeersOf[Worker].toList
    if workers.isEmpty then Map.empty
    else
      val chunkSize = (inputs.size + workers.size - 1) / workers.size
      workers
        .zip(inputs.grouped(chunkSize))
        .toMap
        .map:
          case (worker, tasks) => worker -> tasks.map(new Task(_))

  def program(using Network, Placement, Multitier) = Multitier:
    val allocation = on[Master]:
      allocateTasks

    val workerResults = on[Worker]:
      val localAddress = peerAddress
      asLocal(allocation)
        .filter(_._1 == localAddress)
        .flatMap(_._2)
        .map(_.exec())

    on[Master]:
      val results = asLocalAll(workerResults).values.flatMap(_.toList).toList
      println(s"Master collected results: $results")
```

Best when computation is colocated with data or devices, while cross-placement reads
remain explicit and typed.

</details>

<details>
<summary><b>Collective</b> — neighborhood-based, aggregate computations</summary>

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import io.github.party.Collective
import io.github.party.Collective.*
import io.github.party.network.Network
import io.github.party.network.Network.peerAddress
import io.github.party.peers.Peers.Cardinality.*
import io.github.party.placement.PeerScope.take
import io.github.party.placement.Placement
import io.github.party.placement.PlacementType.on

object AggregateCounter:
  type Smartphone <: { type Tie <: Multiple[Smartphone] }

  def program(using Network, Placement, Collective) =
    val counter = Collective[Smartphone](1.second):
      rep(0)(_ + 1)

    on[Smartphone]:
      take(counter).subscribe: value =>
        println(s"Device ${peerAddress} has count: $value")
```

Best for aggregate computations where each node evolves local state while observing
nearby peers.

</details>

## Examples

The [`examples`](./examples/src/io/github/party) module contains small, runnable programs:

`PingPong` · `RemoteFunction` · `MasterWorker` · `AggregateCounter` · `Broadcasting` ·
`DistAuth` · `BuyerSellerShipper` · `TicTacToe` · `MergeSort`

The [`nebula`](./nebula/src/io/github/party/nebula) module is a larger hybrid demo where:

- **collective** programming computes topology awareness and participation eligibility,
- **choreography** coordinates training rounds and gathers updates,
- **multitier** programming exposes the final state to a dashboard peer.

## Contributing

Contributions are welcome. By participating in this project you agree to abide by its
[Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Camil is distributed under the [MIT License](./LICENSE). Unless stated otherwise, the code
in this repository is provided on an "as is" basis, without warranty of any kind.
