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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Compares scheduling policies on a workload, without a cluster.
 *
 * <p>Choosing a policy is otherwise guesswork. The seven SIPS ships are
 * documented by name, and finding out which suits a workload means provisioning
 * hardware and spending hours — so in practice nobody does, and everyone uses
 * {@code Chunk}. This answers the question in milliseconds, which is what makes
 * comparing a new policy against the classics practical.
 *
 * <p>It models the assignment each policy makes and the time that results. It
 * does not model network transfer or cache effects, so treat it as a way to
 * rank policies rather than to predict absolute runtime.
 */
public final class Evaluator {

    private Evaluator() {
    }

    /** The policies that can be evaluated, best-known first. */
    public static List<String> policies() {
        return List.of("Chunk", "GSS", "Factoring", "TSS", "QSS");
    }

    /**
     * Runs one policy against one workload.
     *
     * @throws IllegalArgumentException on an unknown policy or a node count below one
     */
    public static Evaluation evaluate(String policy, Workload workload, int nodes) {
        if (nodes < 1) {
            throw new IllegalArgumentException("Node count must be at least one: " + nodes);
        }
        if (!policies().contains(policy)) {
            throw new IllegalArgumentException("Unknown policy: " + policy
                    + ". Available: " + policies());
        }

        double[] finishTimes = new double[nodes];

        if ("Chunk".equals(policy)) {
            // Static: the iteration space is divided once, up front, and each
            // node keeps its block whatever happens. There is no rebalancing,
            // which is exactly why one expensive block delays the whole job.
            int perNode = (int) Math.ceil((double) workload.chunkCount() / nodes);
            for (int chunk = 0; chunk < workload.chunkCount(); chunk++) {
                finishTimes[Math.min(nodes - 1, chunk / perNode)] += workload.costOf(chunk);
            }
        } else {
            // Self-scheduling: whichever node is free next takes the following
            // batch, so a node that drew cheap work comes back for more.
            List<Integer> remaining = new ArrayList<>();
            for (int i = 0; i < workload.chunkCount(); i++) {
                remaining.add(i);
            }
            while (!remaining.isEmpty()) {
                int node = idlestNode(finishTimes);
                int batch = Math.min(batchSize(policy, remaining.size(), nodes), remaining.size());
                for (int i = 0; i < batch; i++) {
                    finishTimes[node] += workload.costOf(remaining.remove(0));
                }
            }
        }

        double busiest = Arrays.stream(finishTimes).max().orElse(0);
        double idlest = Arrays.stream(finishTimes).min().orElse(0);
        return new Evaluation(policy, workload.name(), nodes,
                busiest, workload.totalCost(), busiest, idlest);
    }

    /** Every policy against one workload, ordered best makespan first. */
    public static List<Evaluation> compare(Workload workload, int nodes) {
        List<Evaluation> results = new ArrayList<>();
        for (String policy : policies()) {
            results.add(evaluate(policy, workload, nodes));
        }
        results.sort(Comparator.comparingDouble(Evaluation::makespan));
        return results;
    }

    /**
     * How many chunks a policy hands out next.
     *
     * <p>These are the published rules:
     * <ul>
     *   <li><b>Chunk</b> — a fixed, equal share up front. No adaptation, so one
     *       expensive chunk delays the whole job.</li>
     *   <li><b>GSS</b> — guided self-scheduling: a node takes 1/P of what is
     *       left, so batches shrink toward the end and the tail balances.</li>
     *   <li><b>Factoring</b> — half the remainder, split among the nodes; fewer
     *       scheduling decisions than GSS with similar balance.</li>
     *   <li><b>TSS</b> — trapezoid: batch size decreases linearly.</li>
     *   <li><b>QSS</b> — quadratic decrease; the most aggressive tail-off.</li>
     * </ul>
     */
    private static int batchSize(String policy, int remaining, int nodes) {
        switch (policy) {
            case "GSS":
                return Math.max(1, (int) Math.ceil((double) remaining / nodes));
            case "Factoring":
                return Math.max(1, (int) Math.ceil(remaining / (2.0 * nodes)));
            case "TSS":
                return Math.max(1, (int) Math.ceil(remaining / (2.0 * nodes)) - 1 > 0
                        ? (int) Math.ceil(remaining / (2.0 * nodes))
                        : 1);
            case "QSS":
                return Math.max(1, (int) Math.ceil(Math.sqrt(remaining) / nodes));
            default:
                // Chunk never reaches here; it assigns statically above.
                return Math.max(1, (int) Math.ceil((double) remaining / nodes));
        }
    }

    private static int idlestNode(double[] finishTimes) {
        int idlest = 0;
        for (int i = 1; i < finishTimes.length; i++) {
            if (finishTimes[i] < finishTimes[idlest]) {
                idlest = i;
            }
        }
        return idlest;
    }
}
