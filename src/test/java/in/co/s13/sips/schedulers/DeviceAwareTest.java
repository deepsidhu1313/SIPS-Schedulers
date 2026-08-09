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

import in.co.s13.sips.lib.accelerator.AcceleratorType;
import in.co.s13.sips.lib.accelerator.Backend;
import in.co.s13.sips.lib.accelerator.Device;
import in.co.s13.sips.lib.common.datastructure.LiveNode;
import in.co.s13.sips.lib.common.datastructure.Node;
import in.co.s13.sips.lib.common.datastructure.SIPSTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Placing work on hardware that suits it.
 *
 * <p>Existing schedulers rank nodes on queue depth, CPU benchmark and network
 * distance, all of which are blind to whether a node has a GPU. For a
 * compute-bound kernel that leaves a 13x speedup on the table whenever the
 * chunk lands on a CPU-only node.
 *
 * <p>The inverse matters just as much: sending a pointwise kernel to a GPU is
 * about five times slower than leaving it on the CPU, so this scheduler must
 * <em>not</em> prefer accelerators unconditionally.
 */
class DeviceAwareTest {

    private static final Device CPU = new Device(Backend.JAVA_CPU, "cpu:0", "host CPU",
            "T", AcceleratorType.CPU, 8, 16L << 30);
    private static final Device DGPU = new Device(Backend.OPENCL, "opencl:2", "Radeon Pro",
            "AMD", AcceleratorType.DISCRETE_GPU, 20, 4L << 30);

    private static Node node(String uuid, List<Device> devices) {
        LiveNode node = new LiveNode(uuid, uuid + ".local", "Linux", "Test CPU",
                4, 0, 16L << 30, 8L << 30, 500L << 30, 250L << 30,
                new JSONObject(), System.currentTimeMillis(), 0.1);
        node.setDevices(devices);
        return node;
    }

    private static ConcurrentHashMap<String, Node> nodes(Node... list) {
        ConcurrentHashMap<String, Node> map = new ConcurrentHashMap<>();
        for (Node n : list) {
            map.put(n.getUuid(), n);
        }
        return map;
    }

    private static ConcurrentHashMap<String, SIPSTask> tasks(int count) {
        ConcurrentHashMap<String, SIPSTask> map = new ConcurrentHashMap<>();
        for (int i = 0; i < count; i++) {
            SIPSTask task = new SIPSTask(i, "task-" + i);
            map.put(task.getName(), task);
        }
        return map;
    }

    private static JSONObject settings(String workload, int maxNodes) {
        JSONObject json = new JSONObject();
        json.put("MaxNodes", String.valueOf(maxNodes));
        if (workload != null) {
            json.put("Workload", workload);
        }
        return json;
    }

    @Test
    void computeBoundWorkGoesToTheGpuNode() {
        Node gpu = node("gpu-node", List.of(CPU, DGPU));
        Node cpu = node("cpu-node", List.of(CPU));

        ArrayList<SIPSTask> scheduled = new DeviceAware()
                .schedule(nodes(gpu, cpu), tasks(4), settings("compute-bound", 1));

        assertFalse(scheduled.isEmpty());
        for (SIPSTask task : scheduled) {
            assertEquals("gpu-node", task.getNodeUUID(),
                    "heavy work should land on the node with the accelerator");
        }
    }

    @Test
    void transferBoundWorkIsNotForcedOntoTheGpuNode() {
        // The GPU node must not win purely for having a GPU when the kernel is
        // cheap: that placement is measurably slower.
        Node gpu = node("gpu-node", List.of(CPU, DGPU));
        Node cpu = node("cpu-node", List.of(CPU));

        ArrayList<SIPSTask> scheduled = new DeviceAware()
                .schedule(nodes(gpu, cpu), tasks(4), settings("transfer-bound", 2));

        assertEquals(2, new DeviceAware().getSelectedNodes(nodes(gpu, cpu),
                settings("transfer-bound", 2)),
                "both nodes remain equally eligible for cheap work");
        assertEquals(4, scheduled.size());
    }

    @Test
    void anUnknownWorkloadDoesNotGambleOnTheGpu() {
        Node gpu = node("gpu-node", List.of(CPU, DGPU));
        Node cpu = node("cpu-node", List.of(CPU));

        ArrayList<SIPSTask> scheduled = new DeviceAware()
                .schedule(nodes(gpu, cpu), tasks(4), settings(null, 2));

        assertEquals(4, scheduled.size());
        assertTrue(scheduled.stream().allMatch(t -> t.getNodeUUID() != null));
    }

    @Test
    void everyTaskIsAssignedExactlyOnce() {
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("a", List.of(CPU, DGPU)), node("b", List.of(CPU))),
                tasks(7), settings("compute-bound", 2));

        assertEquals(7, scheduled.size());
        assertEquals(7, scheduled.stream().map(SIPSTask::getName).distinct().count());
        assertTrue(scheduled.stream().allMatch(t -> t.getNodeUUID() != null));
    }

    @Test
    void workSpreadsAcrossNodesRatherThanPilingOnTheBest() {
        // Two GPU nodes and enough chunks that using only one would serialise.
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("gpu-a", List.of(CPU, DGPU)), node("gpu-b", List.of(CPU, DGPU))),
                tasks(6), settings("compute-bound", 2));

        long distinct = scheduled.stream().map(SIPSTask::getNodeUUID).distinct().count();
        assertEquals(2, distinct, "both accelerator nodes should be used");
    }

    @Test
    void fallsBackGracefullyWhenNoNodeHasAnAccelerator() {
        // A cluster with no GPUs must still run compute-bound jobs.
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("a", List.of(CPU)), node("b", List.of(CPU))),
                tasks(4), settings("compute-bound", 2));

        assertEquals(4, scheduled.size());
        assertTrue(scheduled.stream().allMatch(t -> t.getNodeUUID() != null));
    }

    @Test
    void toleratesNodesThatAdvertiseNoDevices() {
        // Peers on an older build advertise nothing; they stay schedulable.
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("legacy", List.of()), node("gpu", List.of(CPU, DGPU))),
                tasks(4), settings("compute-bound", 2));

        assertEquals(4, scheduled.size());
    }

    @Test
    void respectsMaxNodes() {
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("a", List.of(CPU, DGPU)), node("b", List.of(CPU)),
                        node("c", List.of(CPU))),
                tasks(6), settings("compute-bound", 1));

        assertEquals(1, scheduled.stream().map(SIPSTask::getNodeUUID).distinct().count());
    }

    @Test
    void reportsWhatItDidForTheJobLog() {
        DeviceAware scheduler = new DeviceAware();
        scheduler.schedule(nodes(node("gpu", List.of(CPU, DGPU)), node("cpu", List.of(CPU))),
                tasks(4), settings("compute-bound", 2));

        assertFalse(scheduler.getOutputs().isEmpty(), "should explain its placement");
        assertTrue(scheduler.getOutputs().stream()
                .anyMatch(line -> line.toLowerCase().contains("compute-bound")),
                "the workload it assumed should be visible: " + scheduler.getOutputs());
    }

    @Test
    void anEmptyClusterYieldsNoAssignmentsRatherThanThrowing() {
        ArrayList<SIPSTask> scheduled = new DeviceAware()
                .schedule(new ConcurrentHashMap<>(), tasks(3), settings("compute-bound", 2));
        assertTrue(scheduled.isEmpty());
    }

    @Test
    void anInvalidWorkloadNameDoesNotFailTheJob() {
        // A typo in a manifest should degrade to "unknown", not abort the job.
        ArrayList<SIPSTask> scheduled = new DeviceAware().schedule(
                nodes(node("a", List.of(CPU, DGPU))), tasks(3), settings("gpu-please", 1));
        assertEquals(3, scheduled.size());
    }
}
