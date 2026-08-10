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
import in.co.s13.sips.schedulers.placement.EarliestFinish;
import in.co.s13.sips.schedulers.placement.NearestData;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether knowing where the data is actually buys anything.
 *
 * <p>The reason read edges exist. If a locality-aware policy cannot be shown to
 * beat one that ignores locality, the whole {@code reads}/{@code writes}
 * apparatus is a field nothing reads — so this measures it rather than asserting
 * it.
 */
class DataLocalityTest {

    /**
     * Two independent producers and a consumer for each.
     *
     * <p>Two producers so they land on different nodes: with one, every policy
     * puts the consumer where the producer was and there is nothing to compare.
     */
    private static Job producerConsumer() {
        Job job = new Job("locality");
        Stage left = job.single("produce-left").writes("left.raw");
        Stage right = job.single("produce-right").writes("right.raw");
        job.single("consume-left").reads(left);
        job.single("consume-right").reads(right);
        return job;
    }

    /** The same shape, but the consumers only wait — they read nothing. */
    private static Job orderedOnly() {
        Job job = new Job("ordered");
        Stage left = job.single("produce-left");
        Stage right = job.single("produce-right");
        job.single("consume-left").after(left);
        job.single("consume-right").after(right);
        return job;
    }

    /**
     * One consumer is much heavier than the other, which is what makes the fast
     * node worth reaching for even when the data is elsewhere. With equal costs
     * both policies co-locate by accident and there is nothing to measure.
     */
    private static final ToDoubleFunction<Stage> COSTS =
            stage -> stage.name().equals("consume-right") ? 30.0 : 10.0;

    /** Fast enough to attract work, slow enough that transfer can outweigh it. */
    private static final Map<String, Double> NODES = Map.of("fast", 1.4, "slow", 1.0);

    /** Moving a volume costs as much as processing one. Not unusual for imaging. */
    private static final double TRANSFER = 10.0;

    @Test
    void aLocalityAwarePolicyBeatsOneThatIgnoresIt() {
        // Each producer leaves its output on the node it ran on. A policy that
        // knows where can keep each consumer there; one that does not will send
        // a consumer to the node that looks faster and pay to move a volume
        // across the network to reach it.
        double blind = DagEvaluator.evaluate(new EarliestFinish(), producerConsumer(),
                COSTS, NODES, TRANSFER).makespan();
        double aware = DagEvaluator.evaluate(new NearestData(TRANSFER), producerConsumer(),
                COSTS, NODES, TRANSFER).makespan();

        assertTrue(aware < blind,
                "knowing where the data is should be worth something: "
                + aware + " vs " + blind);
        assertEquals(40.0, aware, 0.01);
        assertEquals(45.71, blind, 0.01,
                "the blind policy reaches for the faster node and pays to drag "
                + "the volume across to it");
    }

    @Test
    void anOrderingEdgeLeavesNothingToStayNear() {
        // The distinction that makes read edges worth having at all: if the
        // second stage reads nothing, locality has no opinion and the two
        // policies must agree.
        assertEquals(
                DagEvaluator.evaluate(new EarliestFinish(), orderedOnly(), COSTS, NODES, TRANSFER)
                        .makespan(),
                DagEvaluator.evaluate(new NearestData(TRANSFER), orderedOnly(), COSTS, NODES,
                        TRANSFER).makespan(),
                0.001);
    }

    @Test
    void freeTransferMakesLocalityWorthNothing() {
        // The honest boundary. On a cluster with a fast enough fabric, moving
        // the data is not a cost and the whole question is moot.
        assertEquals(
                DagEvaluator.evaluate(new EarliestFinish(), producerConsumer(), COSTS, NODES, 0)
                        .makespan(),
                DagEvaluator.evaluate(new NearestData(0), producerConsumer(), COSTS, NODES, 0)
                        .makespan(),
                0.001);
    }
}
