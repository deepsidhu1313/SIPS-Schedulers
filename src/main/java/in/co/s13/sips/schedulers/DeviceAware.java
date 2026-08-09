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
package in.co.s13.sips.schedulers;

import in.co.s13.sips.lib.accelerator.NodeFitness;
import in.co.s13.sips.lib.accelerator.WorkloadProfile;
import in.co.s13.sips.lib.common.datastructure.LiveNode;
import in.co.s13.sips.lib.common.datastructure.Node;
import in.co.s13.sips.lib.common.datastructure.ParallelForLoop;
import in.co.s13.sips.lib.common.datastructure.ParallelForSENP;
import in.co.s13.sips.lib.common.datastructure.SIPSTask;
import in.co.s13.sips.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

/**
 * Places work on nodes whose hardware suits it.
 *
 * <p>The other schedulers rank nodes on queue depth, CPU benchmark score and
 * network distance, none of which can see whether a node has a GPU. For a
 * compute-bound kernel that leaves a large speedup unclaimed whenever a chunk
 * lands on a CPU-only node — measured at 13.3x for sobel on the reference
 * machine.
 *
 * <p>It does <em>not</em> prefer accelerators unconditionally, because the
 * inverse mistake is worse. A pointwise kernel such as grayscale is dominated by
 * the cost of moving the image to the device and back, and running it on a
 * discrete GPU is about five times slower than leaving it on the CPU. So the
 * job declares its shape and the scheduler ranks accordingly:
 *
 * <pre>{@code
 * "SCHEDULER": {
 *     "Name": "in.co.s13.sips.schedulers.DeviceAware",
 *     "MaxNodes": "4",
 *     "Workload": "compute-bound"
 * }
 * }</pre>
 *
 * <p>{@code Workload} accepts {@code compute-bound}, {@code transfer-bound} or
 * {@code unknown}. Absent or unrecognised, it falls back to {@code unknown},
 * which ranks on CPU capacity alone — a job must not fail over a manifest typo.
 *
 * <p>Within an equally fit group, nodes keep the conventional ordering: shortest
 * queue wait first, then queue length, then CPU score, then network distance.
 * Device fitness is an additional first-order key, not a replacement.
 */
public class DeviceAware implements Scheduler {

    private int totalNodes;
    private int totalChunks;
    private int selectedNodes;
    private final ArrayList<Node> backupNodes = new ArrayList<>();
    private final ArrayList<String> errors = new ArrayList<>();
    private final ArrayList<String> outputs = new ArrayList<>();

    @Override
    public ArrayList<SIPSTask> schedule(ConcurrentHashMap<String, Node> livenodes,
            ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {

        WorkloadProfile workload = workloadFrom(schedulerSettings);
        ArrayList<Node> ranked = rank(livenodes, schedulerSettings, workload);

        ArrayList<SIPSTask> result = new ArrayList<>();
        if (ranked.isEmpty()) {
            errors.add("No live nodes available to schedule on");
            return result;
        }

        ArrayList<SIPSTask> ordered = sortTasksAccordingToDependencies(tasks);
        int nodeCounter = 0;
        for (SIPSTask task : ordered) {
            // Round-robin over the selected nodes: concentrating every chunk on
            // the single fittest node would serialise the job.
            task.setNodeUUID(ranked.get(nodeCounter).getUuid());
            result.add(task);
            nodeCounter = (nodeCounter + 1) % ranked.size();
        }

        this.totalNodes = livenodes.size();
        this.selectedNodes = ranked.size();
        this.totalChunks = result.size();
        backupNodes.addAll(ranked);
        return result;
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> nodes,
            ParallelForLoop loop, JSONObject schedulerSettings) {
        // Chunking a numeric range is unchanged by device awareness; only node
        // selection differs, and Chunk already does the range split well.
        Chunk delegate = new Chunk();
        ArrayList<ParallelForSENP> result = delegate.scheduleParallelFor(
                orderedForLoop(nodes, schedulerSettings), loop, schedulerSettings);
        outputs.addAll(delegate.getOutputs());
        errors.addAll(delegate.getErrors());
        this.totalNodes = delegate.getTotalNodes();
        this.selectedNodes = delegate.getSelectedNodes();
        this.totalChunks = delegate.getTotalChunks();
        return result;
    }

    /**
     * Restricts the map handed to the delegate to the fittest nodes, so range
     * chunking inherits device awareness without duplicating the split logic.
     */
    private ConcurrentHashMap<String, Node> orderedForLoop(ConcurrentHashMap<String, Node> nodes,
            JSONObject schedulerSettings) {
        ArrayList<Node> ranked = rank(nodes, schedulerSettings, workloadFrom(schedulerSettings));
        ConcurrentHashMap<String, Node> selected = new ConcurrentHashMap<>();
        for (Node node : ranked) {
            selected.put(node.getUuid(), node);
        }
        return selected.isEmpty() ? nodes : selected;
    }

    /** Reads the Workload setting, degrading to unknown rather than failing. */
    private WorkloadProfile workloadFrom(JSONObject schedulerSettings) {
        String declared = schedulerSettings == null ? null
                : schedulerSettings.optString("Workload", null);
        try {
            WorkloadProfile profile = WorkloadProfile.byName(declared);
            outputs.add("Workload: " + (declared == null ? "unknown (not declared)" : declared)
                    + " -> " + profile);
            return profile;
        } catch (IllegalArgumentException ex) {
            errors.add("Unrecognised Workload '" + declared + "', treating as unknown: "
                    + ex.getMessage());
            outputs.add("Workload: unknown (unrecognised value '" + declared + "')");
            return WorkloadProfile.unknown();
        }
    }

    /** Fittest first, capped at MaxNodes. */
    private ArrayList<Node> rank(ConcurrentHashMap<String, Node> livenodes,
            JSONObject schedulerSettings, WorkloadProfile workload) {

        ArrayList<Node> nodes = new ArrayList<>(livenodes.values());
        if (nodes.isEmpty()) {
            return nodes;
        }

        // Device fitness first; conventional load and locality ordering after,
        // so equally-equipped nodes still balance sensibly.
        Comparator<Node> byFitness = Comparator
                .comparingDouble((Node n) -> NodeFitness.score(n.getDevices(), workload))
                .reversed();
        nodes.sort(byFitness
                .thenComparing(neutralIfUnavailable(LiveNode.LiveNodeComparator.QWAIT))
                .thenComparing(neutralIfUnavailable(LiveNode.LiveNodeComparator.QLEN.reversed()))
                .thenComparing(neutralIfUnavailable(
                        LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()))
                .thenComparing(neutralIfUnavailable(
                        LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT)));

        int maxNodes = schedulerSettings == null ? 4 : schedulerSettings.getInt("MaxNodes", 4);
        if (maxNodes > 0 && maxNodes < nodes.size()) {
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }

        long withAccelerator = nodes.stream()
                .filter(n -> NodeFitness.hasAccelerator(n.getDevices()))
                .count();
        outputs.add("Selected " + nodes.size() + " of " + livenodes.size() + " node(s), "
                + withAccelerator + " with an accelerator");
        if (workload.prefersAccelerator() && withAccelerator == 0) {
            outputs.add("No node advertises an accelerator; compute-bound work will "
                    + "run on CPUs");
        }
        return nodes;
    }

    /** Dependencies before dependents, matching the other schedulers. */
    private ArrayList<SIPSTask> sortTasksAccordingToDependencies(
            ConcurrentHashMap<String, SIPSTask> tasks) {
        ArrayList<SIPSTask> tasksList = new ArrayList<>(tasks.values());
        Collections.sort(tasksList, SIPSTask.SIPSTaskComparator.NO_OF_DEPENDENCIES
                .thenComparing(SIPSTask.SIPSTaskComparator.ID));

        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask task = tasksList.get(i);
            for (String dependency : task.getDependsOn()) {
                SIPSTask dependencyTask = tasks.get(dependency);
                if (dependencyTask == null) {
                    continue;
                }
                int dependencyIndex = tasksList.indexOf(dependencyTask);
                int taskIndex = tasksList.indexOf(task);
                if (dependencyIndex > taskIndex) {
                    tasksList.remove(dependencyIndex);
                    tasksList.add(taskIndex, dependencyTask);
                    i = 0;
                }
            }
        }
        return tasksList;
    }

    /** How many nodes this scheduler would use, without scheduling anything. */
    public int getSelectedNodes(ConcurrentHashMap<String, Node> livenodes,
            JSONObject schedulerSettings) {
        return rank(livenodes, schedulerSettings, workloadFrom(schedulerSettings)).size();
    }

    @Override
    public ArrayList<Node> getBackupNodes() {
        return backupNodes;
    }

    @Override
    public int getTotalNodes() {
        return totalNodes;
    }

    @Override
    public int getTotalChunks() {
        return totalChunks;
    }

    @Override
    public int getSelectedNodes() {
        return selectedNodes;
    }

    @Override
    public ArrayList<String> getErrors() {
        return errors;
    }

    @Override
    public ArrayList<String> getOutputs() {
        return outputs;
    }

    /**
     * Treats a node as neither better nor worse when the data a comparator
     * needs is missing.
     *
     * <p>The benchmark comparators read into the node's benchmark JSON and
     * throw if it is absent. A node that has not benchmarked yet — one that has
     * only just joined — would otherwise abort scheduling for the whole job
     * rather than simply losing its tiebreak.
     */
    private static Comparator<Node> neutralIfUnavailable(Comparator<Node> comparator) {
        return (left, right) -> {
            try {
                return comparator.compare(left, right);
            } catch (RuntimeException ex) {
                return 0;
            }
        };
    }
}
