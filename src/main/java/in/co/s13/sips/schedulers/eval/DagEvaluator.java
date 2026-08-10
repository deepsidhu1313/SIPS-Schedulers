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
import in.co.s13.sips.lib.job.JobSequencer;
import in.co.s13.sips.lib.job.Stage;
import in.co.s13.sips.lib.job.StageRanks;
import in.co.s13.sips.scheduler.ClusterState;
import in.co.s13.sips.scheduler.PlacementPolicy;
import in.co.s13.sips.scheduler.ReadyTask;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

/**
 * Compares placement policies on a pipeline, without a cluster.
 *
 * <p>The loop-policy counterpart, {@link Evaluator}, exists because choosing a
 * loop policy was otherwise guesswork. The same is true here and worse: DAG
 * scheduling has a large and active literature, and evaluating a new heuristic
 * conventionally means writing a simulator first. Doing it against the same
 * {@link Job} the cluster would run removes that step, and removes the gap
 * between what was simulated and what will actually be scheduled.
 *
 * <p>Models compute time, the ordering constraints between stages, and — when
 * asked — a flat cost for moving a stage's inputs to a node that does not
 * already hold them. It does not model contention or cache, so treat it as a way
 * to rank policies rather than to predict runtime.
 *
 * <p>The whole pipeline is driven through the real {@link JobSequencer}, so a
 * policy is being evaluated against the same release rules the cluster uses
 * rather than a convenient approximation of them.
 */
public final class DagEvaluator {

    private DagEvaluator() {
    }

    /**
     * Runs one policy over one pipeline.
     *
     * @param costOf what a stage costs on a baseline node
     * @param nodeSpeed a node's speed relative to that baseline; 2.0 means it
     *        takes half as long. This is what makes the cluster heterogeneous,
     *        and heterogeneity is the only condition under which the policies
     *        differ at all.
     */
    public static Evaluation evaluate(PlacementPolicy policy, Job job,
            ToDoubleFunction<Stage> costOf, Map<String, Double> nodeSpeed) {
        return evaluate(policy, job, costOf, nodeSpeed, 0);
    }

    /**
     * Runs one policy over one pipeline, charging for data that has to move.
     *
     * @param transferCost what it costs a stage to fetch inputs from a node
     *        other than the one it is running on. Without this the timeline
     *        never punishes a placement that ignores locality, and a
     *        locality-aware policy cannot be shown to be worth anything.
     */
    public static Evaluation evaluate(PlacementPolicy policy, Job job,
            ToDoubleFunction<Stage> costOf, Map<String, Double> nodeSpeed,
            double transferCost) {
        if (transferCost < 0) {
            throw new IllegalArgumentException("transferCost must not be negative: "
                    + transferCost);
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        if (nodeSpeed == null || nodeSpeed.isEmpty()) {
            throw new IllegalArgumentException("A cluster needs at least one node");
        }
        nodeSpeed.forEach((node, speed) -> {
            if (speed <= 0) {
                throw new IllegalArgumentException("Node " + node + " has speed " + speed);
            }
        });

        Map<Stage, Double> ranks = StageRanks.upward(job, costOf);
        JobSequencer sequencer = new JobSequencer(job);
        // Sorted, so which node wins a tie does not depend on how the
        // caller built the map.
        ClusterState cluster = ClusterState.idle(nodeSpeed.keySet().stream().sorted().toList());
        Map<Stage, Double> finishedAt = new LinkedHashMap<>();
        // Where each stage's output ended up, so a locality-aware policy has
        // something real to prefer.
        Map<Stage, String> ranOn = new LinkedHashMap<>();
        double busiest = 0;
        double totalWork = 0;

        while (!sequencer.isFinished()) {
            List<ReadyTask> ready = new ArrayList<>();
            Map<String, Stage> byName = new LinkedHashMap<>();
            for (Stage stage : sequencer.ready()) {
                byName.put(stage.name(), stage);
                ready.add(describe(stage, ranks, finishedAt, ranOn, costOf, nodeSpeed));
            }
            if (ready.isEmpty()) {
                // Every remaining stage is downstream of one that was skipped.
                break;
            }

            for (ReadyTask task : policy.order(ready)) {
                Stage stage = byName.get(task.name());
                Optional<String> chosen = policy.place(task, cluster);
                if (chosen.isEmpty()) {
                    // A policy may decline; the stage stays ready for the next
                    // round rather than being forced somewhere it did not want.
                    continue;
                }
                String node = chosen.get();
                double fetch = transferCost > 0 && !task.inputLocations().isEmpty()
                        && !task.inputLocations().contains(node)
                        ? transferCost
                        : 0;
                double start = Math.max(cluster.availableAt(node), task.readyAt()) + fetch;
                double finish = start + task.costOn(node);

                sequencer.started(stage);
                sequencer.completed(stage);
                finishedAt.put(stage, finish);
                ranOn.put(stage, node);
                cluster = cluster.busyUntil(node, finish);
                busiest = Math.max(busiest, finish);
                totalWork += task.costOn(node);
            }
        }

        double idlest = cluster.nodes().stream()
                .mapToDouble(cluster::availableAt).min().orElse(0);
        return new Evaluation(policy.name(), job.name(), nodeSpeed.size(),
                busiest, totalWork, busiest, idlest);
    }

    private static ReadyTask describe(Stage stage, Map<Stage, Double> ranks,
            Map<Stage, Double> finishedAt, Map<Stage, String> ranOn,
            ToDoubleFunction<Stage> costOf, Map<String, Double> nodeSpeed) {

        double predecessorsDoneAt = 0;
        for (Stage dependency : stage.dependencies()) {
            predecessorsDoneAt = Math.max(predecessorsDoneAt,
                    finishedAt.getOrDefault(dependency, 0.0));
        }

        double baseCost = costOf.applyAsDouble(stage);
        ReadyTask.Builder task = ReadyTask.named(stage.name())
                .cost(baseCost)
                .upwardRank(ranks.get(stage))
                .readyAt(predecessorsDoneAt);
        nodeSpeed.forEach((node, speed) -> task.costOn(node, baseCost / speed));
        // Only stages this one reads from -- a stage it merely waits for left
        // nothing here worth staying near.
        for (Stage producer : stage.inputs()) {
            String where = ranOn.get(producer);
            if (where != null) {
                task.inputAt(where);
            }
        }
        return task.build();
    }

    /** Every policy against one pipeline, best makespan first. */
    public static List<Evaluation> compare(List<PlacementPolicy> policies, Job job,
            ToDoubleFunction<Stage> costOf, Map<String, Double> nodeSpeed) {
        return compare(policies, job, costOf, nodeSpeed, 0);
    }

    /** Every policy against one pipeline, charging for data that has to move. */
    public static List<Evaluation> compare(List<PlacementPolicy> policies, Job job,
            ToDoubleFunction<Stage> costOf, Map<String, Double> nodeSpeed,
            double transferCost) {
        List<Evaluation> results = new ArrayList<>();
        for (PlacementPolicy policy : policies) {
            results.add(evaluate(policy, job, costOf, nodeSpeed, transferCost));
        }
        results.sort(Comparator.comparingDouble(Evaluation::makespan));
        return results;
    }

    /**
     * The shortest this pipeline could possibly take: its critical path on the
     * fastest node available.
     *
     * <p>Worth knowing before blaming a policy. No placement can beat it, and a
     * policy already at it has nothing left to win.
     */
    public static double lowerBound(Job job, ToDoubleFunction<Stage> costOf,
            Map<String, Double> nodeSpeed) {
        double fastest = nodeSpeed.values().stream()
                .mapToDouble(Double::doubleValue).max().orElse(1);
        return StageRanks.criticalPathLength(job, costOf) / fastest;
    }
}
