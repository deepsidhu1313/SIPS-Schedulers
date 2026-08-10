/*
 * Copyright (C) 2026 Navdeep Singh Sidhu
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package in.co.s13.sips.schedulers.placement;

import in.co.s13.sips.scheduler.ClusterState;
import in.co.s13.sips.scheduler.PlacementPolicy;
import in.co.s13.sips.scheduler.ReadyTask;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Choosing a node for a task.
 *
 * <p>Each policy is here for a reason it can be shown to have, on a case where
 * the others get it wrong. A heuristic that never differs from the obvious one
 * is not a heuristic worth its estimates, so these tests are built around the
 * conditions that separate them — a heterogeneous cluster, an uneven graph, data
 * that is already somewhere.
 */
class PlacementPolicyTest {

    /** Two nodes: "fast" is three times "slow". */
    private static ReadyTask task(String name, double onFast, double onSlow) {
        return ReadyTask.named(name).cost(onSlow)
                .costOn("fast", onFast).costOn("slow", onSlow).build();
    }

    private static ClusterState twoNodes() {
        return ClusterState.idle(Set.of("fast", "slow"));
    }

    // ---- least loaded ----

    @Test
    void leastLoadedTakesWhicheverNodeIsFreeFirst() {
        ClusterState cluster = twoNodes().busyUntil("fast", 100);

        assertEquals("slow", new LeastLoaded().place(task("t", 1, 3), cluster).orElseThrow());
    }

    @Test
    void leastLoadedIgnoresHowFastANodeIs() {
        // Its whole limitation, stated as a test: a node free now but ten times
        // slower still wins, and the task finishes later for it.
        ClusterState cluster = twoNodes().busyUntil("fast", 1);
        ReadyTask slowEverywhereButFast = task("t", 1, 100);

        assertEquals("slow",
                new LeastLoaded().place(slowEverywhereButFast, cluster).orElseThrow());
    }

    // ---- earliest finish ----

    @Test
    void earliestFinishPrefersABusyFastNodeOverAFreeSlowOne() {
        // The case that justifies the estimates. fast is busy until 1 and takes
        // 1 (done at 2); slow is free and takes 100 (done at 100).
        ClusterState cluster = twoNodes().busyUntil("fast", 1);

        assertEquals("fast",
                new EarliestFinish().place(task("t", 1, 100), cluster).orElseThrow());
    }

    @Test
    void earliestFinishStillTakesTheFreeNodeWhenTheFastOneIsFarBehind() {
        // Not a blind preference for the fast node: if it is hours behind, the
        // slow node genuinely finishes first.
        ClusterState cluster = twoNodes().busyUntil("fast", 1000);

        assertEquals("slow",
                new EarliestFinish().place(task("t", 1, 10), cluster).orElseThrow());
    }

    @Test
    void aTaskCannotStartBeforeItsInputsExist() {
        // readyAt is when the predecessors finished. A node free at 0 does not
        // get to start at 0 on work that is not ready.
        ReadyTask waiting = ReadyTask.named("t").cost(5).readyAt(50).build();
        ClusterState cluster = ClusterState.idle(Set.of("only"));

        // Placed anyway -- there is nowhere else -- but the finish time the
        // policy reasons with accounts for the wait.
        assertEquals("only", new EarliestFinish().place(waiting, cluster).orElseThrow());
    }

    // ---- nearest data ----

    @Test
    void nearestDataPrefersTheNodeHoldingTheInputs() {
        // Both nodes equal on compute; only the data breaks the tie.
        ReadyTask task = ReadyTask.named("register").cost(10).inputAt("slow").build();

        assertEquals("slow",
                new NearestData(5).place(task, twoNodes()).orElseThrow());
    }

    @Test
    void nearestDataStillMovesTheDataWhenHoldingItWouldCostMore() {
        // A discount, not a rule. If the node holding the data is an hour
        // behind, fetching is cheaper than waiting for it.
        ReadyTask task = ReadyTask.named("register").cost(10).inputAt("slow").build();
        ClusterState cluster = twoNodes().busyUntil("slow", 1000);

        assertEquals("fast", new NearestData(5).place(task, cluster).orElseThrow());
    }

    @Test
    void aTaskWithNoKnownInputLocationIsPlacedOnMerit() {
        ReadyTask task = task("t", 1, 100);

        assertEquals("fast", new NearestData(5).place(task, twoNodes()).orElseThrow());
    }

    @Test
    void aZeroTransferCostMakesNearestDataIndistinguishableFromEarliestFinish() {
        // The honest boundary: with free transfer, locality is worth nothing.
        ReadyTask task = ReadyTask.named("t").cost(10).costOn("fast", 1).inputAt("slow").build();

        assertEquals(new EarliestFinish().place(task, twoNodes()),
                new NearestData(0).place(task, twoNodes()));
    }

    @Test
    void transferCostMustMakeSense() {
        assertThrows(IllegalArgumentException.class, () -> new NearestData(-1));
    }

    // ---- HEFT's ordering ----

    @Test
    void heftConsidersTheMostCriticalTaskFirst() {
        // The half EarliestFinish does not have. Three ready tasks, and the one
        // with the longest chain behind it must go out first -- everything that
        // follows it is waiting.
        List<ReadyTask> ready = List.of(
                ReadyTask.named("leaf").cost(1).upwardRank(1).build(),
                ReadyTask.named("gateway").cost(1).upwardRank(500).build(),
                ReadyTask.named("middling").cost(1).upwardRank(50).build());

        List<String> order = new Heft().order(ready).stream().map(ReadyTask::name).toList();

        assertEquals(List.of("gateway", "middling", "leaf"), order);
    }

    @Test
    void earliestFinishLeavesTheOrderAlone() {
        // Which is precisely what HEFT improves on.
        List<ReadyTask> ready = List.of(
                ReadyTask.named("leaf").cost(1).upwardRank(1).build(),
                ReadyTask.named("gateway").cost(1).upwardRank(500).build());

        assertEquals(ready, new EarliestFinish().order(ready));
    }

    @Test
    void tiedRanksOrderTheSameWayEveryRun() {
        // A heuristic whose result depends on hash order is not reproducible,
        // and an unreproducible benchmark is not evidence.
        List<ReadyTask> ready = List.of(
                ReadyTask.named("zebra").cost(1).upwardRank(10).build(),
                ReadyTask.named("alpha").cost(1).upwardRank(10).build());

        assertEquals(List.of("alpha", "zebra"),
                new Heft().order(ready).stream().map(ReadyTask::name).toList());
    }

    // ---- the SPI itself ----

    @Test
    void aPolicyIsOneMethodAndAName() {
        // The bargain: this is a complete, working policy.
        PlacementPolicy alphabetical = new PlacementPolicy() {
            @Override
            public String name() {
                return "Alphabetical";
            }

            @Override
            public Optional<String> place(ReadyTask task, ClusterState cluster) {
                return cluster.nodes().stream().sorted().findFirst();
            }
        };

        assertEquals("fast", alphabetical.place(task("t", 1, 1), twoNodes()).orElseThrow());
        assertTrue(alphabetical.description().contains("Alphabetical"));
        assertEquals(2, alphabetical.order(List.of(task("a", 1, 1), task("b", 1, 1))).size());
    }

    @Test
    void aPolicyMayDeclineToPlaceATask() {
        // A real answer, not a failure: waiting for a better node can beat
        // taking the first free one.
        PlacementPolicy picky = new PlacementPolicy() {
            @Override
            public String name() {
                return "OnlyFast";
            }

            @Override
            public Optional<String> place(ReadyTask task, ClusterState cluster) {
                return cluster.nodes().contains("fast") && cluster.availableAt("fast") == 0
                        ? Optional.of("fast")
                        : Optional.empty();
            }
        };

        assertTrue(picky.place(task("t", 1, 1), twoNodes().busyUntil("fast", 5)).isEmpty());
    }

    // ---- the state a policy reasons about ----

    @Test
    void clusterStateIsImmutable() {
        // A policy that could change what it is reasoning about would make its
        // decisions depend on the order it happened to consider things in.
        ClusterState original = twoNodes();

        ClusterState updated = original.busyUntil("fast", 42);

        assertEquals(0, original.availableAt("fast"));
        assertEquals(42, updated.availableAt("fast"));
    }

    @Test
    void askingAboutANodeThatIsNotThereIsAnError() {
        assertThrows(IllegalArgumentException.class, () -> twoNodes().availableAt("ghost"));
        assertThrows(IllegalArgumentException.class, () -> twoNodes().busyUntil("ghost", 1));
    }

    @Test
    void anEmptyClusterIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> ClusterState.idle(Set.of()));
        assertThrows(IllegalArgumentException.class, () -> ClusterState.idle(null));
    }

    @Test
    void anUnmeasuredNodeFallsBackToTheDefaultCost() {
        // A cluster with one unbenchmarked machine must still schedule.
        ReadyTask task = ReadyTask.named("t").cost(7).costOn("fast", 1).build();

        assertEquals(7, task.costOn("brand-new-node"));
        assertEquals(1, task.costOn("fast"));
    }

    @Test
    void unmeasuredPowerIsZeroRatherThanAttractive() {
        // Treating unknown as cheap would send everything to the node nobody
        // measured.
        assertEquals(0, twoNodes().wattsOf("fast"));
        assertEquals(95, twoNodes().withWatts(java.util.Map.of("fast", 95.0)).wattsOf("fast"));
    }

    @Test
    void aTaskNeedsANameAndSensibleNumbers() {
        assertThrows(IllegalArgumentException.class, () -> ReadyTask.named(" "));
        assertThrows(IllegalArgumentException.class, () -> ReadyTask.named("t").cost(-1));
        assertThrows(IllegalArgumentException.class, () -> ReadyTask.named("t").readyAt(-1));
        assertThrows(IllegalArgumentException.class, () -> ReadyTask.named("t").costOn("n", -1));
    }
}
