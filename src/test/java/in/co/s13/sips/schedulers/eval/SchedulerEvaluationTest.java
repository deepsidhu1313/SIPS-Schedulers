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

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comparing scheduling policies without a cluster.
 *
 * <p>Choosing a scheduler is currently guesswork: the seven policies are
 * documented by name and nothing says which suits a given workload. Answering
 * that properly needs a cluster, a workload, and hours — so nobody does it, and
 * everyone uses {@code Chunk}.
 *
 * <p>These evaluate a policy against a described workload analytically, in
 * milliseconds. That is what makes scheduling research practical here: a new
 * policy can be measured against the classics before anyone provisions
 * hardware.
 */
class SchedulerEvaluationTest {

    /** Every chunk costs the same — the easy case. */
    private static Workload uniform(int chunks) {
        return Workload.uniform("uniform", chunks, 100);
    }

    /**
     * Cost varies hugely between chunks.
     *
     * <p>This is the case that separates policies. Mandelbrot is the classic
     * example: a pixel inside the set iterates to the limit while one outside
     * exits almost immediately, so cost per chunk varies by orders of
     * magnitude. Static assignment cannot cope; self-scheduling can.
     */
    private static Workload irregular(int chunks) {
        return Workload.skewed("mandelbrot-like", chunks, 5, 500);
    }

    @Test
    void everyPolicyHandlesUniformWorkWell() {
        // With equal chunks there is little to choose between policies; any
        // that does badly here is broken.
        for (String policy : Evaluator.policies()) {
            Evaluation result = Evaluator.evaluate(policy, uniform(64), 8);

            assertTrue(result.efficiency() > 0.75,
                    policy + " wasted more than a quarter of the cluster on "
                    + "uniform work: efficiency " + result.efficiency());
        }
    }

    /**
     * The finding this whole harness exists to make visible.
     */
    @Test
    void selfSchedulingBeatsStaticChunkingOnIrregularWork() {
        Evaluation chunk = Evaluator.evaluate("Chunk", irregular(64), 8);
        Evaluation gss = Evaluator.evaluate("GSS", irregular(64), 8);

        assertTrue(gss.makespan() < chunk.makespan(),
                "GSS should finish sooner than static Chunk on skewed work: "
                + "GSS " + gss.makespan() + " vs Chunk " + chunk.makespan());
        assertTrue(gss.loadImbalance() < chunk.loadImbalance(),
                "GSS should balance better: " + gss.loadImbalance()
                + " vs " + chunk.loadImbalance());
    }

    @Test
    void theAdvantageGrowsWithSkew() {
        // A policy chosen on mildly irregular work may be the wrong one when
        // the spread widens, which is why the harness reports across a range.
        double mildGain = gainOverChunk(Workload.skewed("mild", 64, 80, 120), 8);
        double severeGain = gainOverChunk(Workload.skewed("severe", 64, 5, 500), 8);

        assertTrue(severeGain > mildGain,
                "self-scheduling should pay off more as skew increases: "
                + severeGain + " vs " + mildGain);
    }

    private static double gainOverChunk(Workload workload, int nodes) {
        double chunk = Evaluator.evaluate("Chunk", workload, nodes).makespan();
        double gss = Evaluator.evaluate("GSS", workload, nodes).makespan();
        return chunk / gss;
    }

    @Test
    void reportsSpeedupAndEfficiencyAgainstOneNode() {
        Evaluation single = Evaluator.evaluate("Chunk", uniform(64), 1);
        Evaluation eight = Evaluator.evaluate("Chunk", uniform(64), 8);

        assertEquals(1.0, single.speedup(), 0.01, "one node is the baseline");
        assertTrue(eight.speedup() > 4, "eight nodes should beat four-fold speedup on "
                + "uniform work, got " + eight.speedup());
        assertTrue(eight.speedup() <= 8.0001, "speedup cannot exceed the node count");
    }

    @Test
    void loadImbalanceIsZeroWhenEveryNodeGetsEqualWork() {
        // 64 equal chunks over 8 nodes divides exactly.
        assertEquals(0.0, Evaluator.evaluate("Chunk", uniform(64), 8).loadImbalance(), 0.001);
    }

    @Test
    void aSingleNodeIsPerfectlyBalancedByDefinition() {
        assertEquals(0.0, Evaluator.evaluate("Chunk", irregular(64), 1).loadImbalance(), 0.001);
    }

    @Test
    void comparesEveryPolicyInOneRun() {
        // The output a researcher actually wants: one table, all policies.
        List<Evaluation> table = Evaluator.compare(irregular(64), 8);

        assertEquals(Evaluator.policies().size(), table.size());
        // Sorted best-first so the answer is the top row.
        for (int i = 1; i < table.size(); i++) {
            assertTrue(table.get(i - 1).makespan() <= table.get(i).makespan(),
                    "results should be ordered by makespan");
        }
    }

    @Test
    void resultsAreReproducible() {
        // A benchmark nobody can reproduce is not evidence.
        Evaluation first = Evaluator.evaluate("GSS", irregular(64), 8);
        Evaluation second = Evaluator.evaluate("GSS", irregular(64), 8);

        assertEquals(first.makespan(), second.makespan(), 0.0001);
    }

    @Test
    void rejectsAnUnknownPolicyByName() {
        IllegalArgumentException thrown = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                        () -> Evaluator.evaluate("NoSuchPolicy", uniform(8), 2));
        assertTrue(thrown.getMessage().contains("Chunk"),
                "should name the available policies: " + thrown.getMessage());
    }

    @Test
    void rejectsNonsenseConfigurations() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Evaluator.evaluate("Chunk", uniform(8), 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Workload.uniform("x", 0, 10));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Workload.skewed("x", 4, 100, 10));
    }
}
