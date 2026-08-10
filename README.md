# SIPS-Schedulers

[![build](https://github.com/deepsidhu1313/SIPS-Schedulers/actions/workflows/build.yml/badge.svg)](https://github.com/deepsidhu1313/SIPS-Schedulers/actions/workflows/build.yml)

The scheduling policies SIPS ships, and the harness for comparing a new one
against them without a cluster.

## Two different questions

SIPS asks two, and they are deliberately not the same interface.

**How big is the next batch?** — `LoopPolicy`, for one iteration space split
across nodes. One method:

```java
public long nextBatchSize(long remaining, int nodes, int round) {
    return Math.max(1, (long) Math.ceil((double) remaining / nodes));   // GSS
}
```

**Which ready task goes where?** — `PlacementPolicy`, for a pipeline of stages
with dependencies. Also one method:

```java
public Optional<String> place(ReadyTask task, ClusterState cluster) { ... }
```

Forcing one shape over both would restore exactly the barrier these were written
to remove: eight methods between having an idea and finding out whether it works.

## What ships

### Loop policies

| Policy | Rule | Notes |
|---|---|---|
| `Chunk` | equal shares, assigned up front | genuinely static — no rebalancing, so one expensive block delays the job |
| `GSS` | 1/P of what remains | Polychronopoulos & Kuck; batches shrink toward the end |
| `Factoring` | half the remainder, split P ways | Hummel; fewer decisions than GSS, similar balance |
| `TSS` | linear decrease | Tzen & Ni |
| `QSS` | quadratic decrease | the most aggressive tail-off |
| `GA` | genetic search over assignments | |
| `GATDS` | genetic, dependency-aware | |
| `DeviceAware` | ranks by node fitness first | wraps another policy; missing benchmark data does not abort scheduling |

### Placement policies

| Policy | Rule |
|---|---|
| `LeastLoaded` | whichever node frees up first — the baseline worth beating |
| `EarliestFinish` | whichever node would *finish* soonest, which is not the same node |
| `Heft` | orders ready tasks by upward rank, then EFT (Topcuoglu, Hariri & Wu) |
| `NearestData` | EFT, counting the cost of fetching inputs a node does not hold |

## Comparing policies without a cluster

Choosing a policy is otherwise guesswork: finding out which suits a workload
means provisioning hardware and spending an afternoon, so in practice nobody
does and everyone uses `Chunk`.

```java
Evaluator.compare(Workload.skewed("mandelbrot", 256, 3, 470), 8)
        .forEach(System.out::println);           // loop policies

DagEvaluator.compare(List.of(new Heft(), new EarliestFinish(), new LeastLoaded()),
        job, costOf, nodeSpeeds)                 // placement policies
        .forEach(System.out::println);
```

`DagEvaluator` drives the pipeline through the real `JobSequencer`, so a policy
is measured against the same release rules the cluster uses rather than a
convenient approximation of them.

### Some measured results

From this repository's own test suite, on a three-stage chain beside a single
leaf, two nodes with one half again as fast as the other:

| policy | makespan |
|---|---|
| HEFT | 20.67 |
| EarliestFinish | 24.00 |
| critical-path floor | 20.67 |

The 16% is ordering alone — both use the same node-choice rule. HEFT gives the
fast node to the chain everything else waits on.

And with transfer costing as much as processing, on two producers and two
consumers: `NearestData` 40.0 against `EarliestFinish` 45.71.

### Three honest boundaries

- **On a uniform cluster every placement policy ties.** Heterogeneity is the
  only thing that separates them.
- **This HEFT is insertion-less.** It never backfills an idle gap it created, so
  where one node is much faster it piles a queue onto it — and at a 3× spread on
  that graph, plain `LeastLoaded` wins. There is a test asserting exactly that.
- **The evaluators model compute and transfer, not contention or cache.** Use
  them to rank policies, not to predict runtime.

## Build

```bash
./mvnw verify
```

Requires **JDK 21**.

## Writing your own

Implement the one method, drop the class on the classpath, and name it in the
job's manifest under `SCHEDULER`. Compare it offline first — a policy that
cannot beat `LeastLoaded` is not worth its estimates.

Further reading: [Task graphs](../SIPS-lib/docs/TASK_GRAPHS.md).
