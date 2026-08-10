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
import java.util.Optional;

/**
 * Puts a task on whichever node would finish it soonest.
 *
 * <p>Finishing, not starting. The distinction is the entire point: the node free
 * first is not the node done first if it is three times slower, and a policy
 * that reasons about availability alone will keep feeding a slow machine work
 * that a busy fast one would have finished earlier.
 *
 * <p>Half of HEFT — the half that chooses a node. {@link Heft} adds the other
 * half, which chooses the order.
 */
public class EarliestFinish implements PlacementPolicy {

    @Override
    public String name() {
        return "EarliestFinish";
    }

    @Override
    public Optional<String> place(ReadyTask task, ClusterState cluster) {
        String best = null;
        double bestFinish = Double.MAX_VALUE;
        for (String node : cluster.nodes()) {
            double finish = finishTimeOn(task, cluster, node);
            if (finish < bestFinish) {
                bestFinish = finish;
                best = node;
            }
        }
        return Optional.ofNullable(best);
    }

    /** When this node would be done, if it took the task now. */
    protected double finishTimeOn(ReadyTask task, ClusterState cluster, String node) {
        return Math.max(cluster.availableAt(node), task.readyAt()) + task.costOn(node);
    }

    @Override
    public String description() {
        return "Assigns each task to the node with the earliest estimated finish time";
    }
}
