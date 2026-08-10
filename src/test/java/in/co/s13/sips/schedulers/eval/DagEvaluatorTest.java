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
package in.co.s13.sips.schedulers.eval;

import in.co.s13.sips.lib.job.Job;
import in.co.s13.sips.lib.job.Stage;
import in.co.s13.sips.lib.job.StageRanks;
import in.co.s13.sips.scheduler.PlacementPolicy;
import in.co.s13.sips.schedulers.placement.EarliestFinish;
import in.co.s13.sips.schedulers.placement.Heft;
import in.co.s13.sips.schedulers.placement.LeastLoaded;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comparing placement policies without a cluster.
 *
 * <p>The point of the evaluator is to answer "which policy suits this pipeline"
 * in milliseconds rather than in an afternoon of provisioning — so the thing
 * worth testing is that its answers are real. Each case below is one where the
 * policies genuinely differ and the reason can be stated.
 */
class DagEvaluatorTest {

    /** One node half again as fast as the other: a modest, realistic spread. */
    private static final Map<String, Double> HETEROGENEOUS =
            Map.of("fast", 1.5, "slow", 1.0);

    /**
     * A single leaf beside a three-stage chain, both released together.
     *
     * @param chainFirst whether the chain is declared before the leaf. It should
     *        not matter — which is the point of the tests that vary it.
     */
    private static Job unevenBranches(boolean chainFirst) {
        Job job = new Job("uneven");
        Stage root = job.single("root");
        if (!chainFirst) {
            job.single("leaf").after(root);
        }
        Stage first = job.single("chain-1").after(root);
        Stage second = job.single("chain-2").after(first);
        job.single("chain-3").after(second);
        if (chainFirst) {
            job.single("leaf").after(root);
        }
        return job;
    }

    private static Job unevenBranches() {
        return unevenBranches(false);
    }

    private static ToDoubleFunction<Stage> costs(Map<String, Double> byName) {
        return stage -> byName.getOrDefault(stage.name(), 1.0);
    }

    private static final ToDoubleFunction<Stage> UNEVEN_COSTS = costs(Map.of(
            "root", 1.0, "leaf", 10.0,
            "chain-1", 10.0, "chain-2", 10.0, "chain-3", 10.0));

    @Test
    void orderingByCriticalityIsWorthSixteenPercentHere() {
        // Same node-choice rule in both; the only difference is which of the two
        // ready tasks is considered first. Plain EFT takes them as declared and
        // gives the fast node to the leaf, leaving the three-stage chain -- the
        // thing everything else waits on -- to the slow one.
        double heft = DagEvaluator.evaluate(new Heft(), unevenBranches(),
                UNEVEN_COSTS, HETEROGENEOUS).makespan();
        double unordered = DagEvaluator.evaluate(new EarliestFinish(), unevenBranches(),
                UNEVEN_COSTS, HETEROGENEOUS).makespan();

        assertEquals(20.667, heft, 0.01);
        assertEquals(24.0, unordered, 0.01);
        assertTrue(heft < unordered, heft + " vs " + unordered);
    }

    @Test
    void heftDoesNotCareWhatOrderThePipelineWasWrittenIn() {
        // The property that makes the ranking worth having. Whether the author
        // happened to declare the leaf first or last changes nothing.
        assertEquals(
                DagEvaluator.evaluate(new Heft(), unevenBranches(false),
                        UNEVEN_COSTS, HETEROGENEOUS).makespan(),
                DagEvaluator.evaluate(new Heft(), unevenBranches(true),
                        UNEVEN_COSTS, HETEROGENEOUS).makespan(),
                0.001);
    }

    @Test
    void theSimplerPoliciesDoCareWhatOrderItWasWrittenIn() {
        // Which is a real cost: the same pipeline, written two equally sensible
        // ways, schedules differently and nobody can see why from the file.
        double leafFirst = DagEvaluator.evaluate(new LeastLoaded(), unevenBranches(false),
                UNEVEN_COSTS, HETEROGENEOUS).makespan();
        double chainFirst = DagEvaluator.evaluate(new LeastLoaded(), unevenBranches(true),
                UNEVEN_COSTS, HETEROGENEOUS).makespan();

        assertTrue(Math.abs(leafFirst - chainFirst) > 1,
                "LeastLoaded should be order-sensitive here: " + leafFirst
                + " vs " + chainFirst);
    }

    @Test
    void greedilyFillingTheFastNodeCanLoseToSimplySpreadingOut() {
        // Worth stating rather than hiding. This EFT is insertion-less: it never
        // backfills an idle gap it created, so when one node is much faster it
        // piles a queue onto it while the other sits idle. On this graph at a
        // 3x spread, plain LeastLoaded wins.
        Map<String, Double> wideSpread = Map.of("fast", 3.0, "slow", 1.0);

        double heft = DagEvaluator.evaluate(new Heft(), unevenBranches(),
                UNEVEN_COSTS, wideSpread).makespan();
        double spread = DagEvaluator.evaluate(new LeastLoaded(), unevenBranches(),
                UNEVEN_COSTS, wideSpread).makespan();

        assertTrue(spread < heft,
                "expected the naive policy to win here: " + spread + " vs " + heft);
    }

    @Test
    void onAUniformClusterEveryPolicyAgrees() {
        // The honest boundary. Heterogeneity is the only thing that separates
        // these policies; on identical machines the estimates buy nothing, and a
        // test suite that never says so is overselling them.
        Map<String, Double> identical = Map.of("a", 1.0, "b", 1.0, "c", 1.0);
        Job job = new Job("wide");
        Stage root = job.single("root");
        for (int i = 0; i < 6; i++) {
            job.single("leaf-" + i).after(root);
        }
        ToDoubleFunction<Stage> equalCost = stage -> 10.0;

        List<Evaluation> results = DagEvaluator.compare(
                List.of(new Heft(), new EarliestFinish(), new LeastLoaded()),
                job, equalCost, identical);

        assertEquals(results.get(0).makespan(), results.get(2).makespan(), 0.001,
                "with identical nodes there is nothing to choose between them");
    }

    @Test
    void noPolicyCanBeatTheCriticalPath() {
        // A floor worth knowing before blaming a scheduler: the chain is 31
        // units of work that cannot overlap with itself.
        double floor = DagEvaluator.lowerBound(unevenBranches(), UNEVEN_COSTS, HETEROGENEOUS);

        for (PlacementPolicy policy : List.of(new Heft(), new EarliestFinish(),
                new LeastLoaded())) {
            Evaluation result = DagEvaluator.evaluate(policy, unevenBranches(),
                    UNEVEN_COSTS, HETEROGENEOUS);
            assertTrue(result.makespan() >= floor - 0.001,
                    policy.name() + " claims " + result.makespan()
                    + ", below the critical path " + floor);
        }
    }

    @Test
    void aChainCanOnlyEverRunOneStageAtATime() {
        // Sanity: no amount of cluster helps a pipeline with no parallelism, and
        // a policy reporting otherwise has a bug in its ordering constraints.
        Job chain = new Job("chain");
        Stage a = chain.single("a");
        Stage b = chain.single("b").after(a);
        chain.single("c").after(b);

        Evaluation result = DagEvaluator.evaluate(new Heft(), chain,
                stage -> 10.0, Map.of("only", 1.0, "spare", 1.0));

        assertEquals(30, result.makespan(), 0.001);
    }

    @Test
    void independentStagesActuallyRunAtTheSameTime() {
        Job wide = new Job("wide");
        wide.single("a");
        wide.single("b");

        Evaluation result = DagEvaluator.evaluate(new Heft(), wide,
                stage -> 10.0, Map.of("one", 1.0, "two", 1.0));

        assertEquals(10, result.makespan(), 0.001,
                "two independent stages on two nodes take one stage's time");
    }

    @Test
    void aComparisonIsOrderedBestFirst() {
        List<Evaluation> results = DagEvaluator.compare(
                List.of(new LeastLoaded(), new Heft(), new EarliestFinish()),
                unevenBranches(), UNEVEN_COSTS, HETEROGENEOUS);

        assertEquals(3, results.size());
        for (int i = 1; i < results.size(); i++) {
            assertTrue(results.get(i - 1).makespan() <= results.get(i).makespan());
        }
    }

    @Test
    void nonsenseIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> DagEvaluator.evaluate(
                null, unevenBranches(), UNEVEN_COSTS, HETEROGENEOUS));
        assertThrows(IllegalArgumentException.class, () -> DagEvaluator.evaluate(
                new Heft(), unevenBranches(), UNEVEN_COSTS, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> DagEvaluator.evaluate(
                new Heft(), unevenBranches(), UNEVEN_COSTS, Map.of("broken", 0.0)));
    }

    // ---- ranks ----

    @Test
    void aStagesRankIsItsOwnCostPlusTheLongestChainAhead() {
        Map<Stage, Double> ranks = StageRanks.upward(unevenBranches(), UNEVEN_COSTS);
        Job job = unevenBranches();

        // Recompute against a fresh graph by name, since Stage identity is
        // per-job.
        Map<String, Double> byName = new java.util.LinkedHashMap<>();
        ranks.forEach((stage, rank) -> byName.put(stage.name(), rank));

        assertEquals(10, byName.get("chain-3"), 0.001, "nothing follows it");
        assertEquals(20, byName.get("chain-2"), 0.001);
        assertEquals(30, byName.get("chain-1"), 0.001);
        assertEquals(10, byName.get("leaf"), 0.001);
        assertEquals(31, byName.get("root"), 0.001, "root plus the longest chain, not the leaf");
        assertEquals(5, job.stages().size());
    }

    @Test
    void theCriticalPathIsTheLongestChainThroughTheGraph() {
        assertEquals(31, StageRanks.criticalPathLength(unevenBranches(), UNEVEN_COSTS), 0.001);
    }

    @Test
    void aRankNeedsACostFunction() {
        assertThrows(IllegalArgumentException.class,
                () -> StageRanks.upward(unevenBranches(), null));
    }

    @Test
    void aCyclicGraphCannotBeRanked() {
        Job job = new Job("looped");
        Stage a = job.single("a");
        Stage b = job.single("b").after(a);
        a.after(b);

        assertThrows(IllegalStateException.class, () -> StageRanks.upward(job, stage -> 1.0));
    }
}
