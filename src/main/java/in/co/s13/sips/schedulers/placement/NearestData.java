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
import in.co.s13.sips.scheduler.ReadyTask;

/**
 * Prefers a node that already holds the task's inputs, and otherwise finishes
 * soonest.
 *
 * <p>A pipeline stage reads what the stage before it wrote. Moving that data is
 * work nobody asked for, and on a volume of any size it dominates the compute —
 * so a placement that ignores where the bytes already are can lose to one that
 * picks a slower node holding them.
 *
 * <p>Expressed as a discount rather than a rule: holding the data makes a node
 * look faster, but a node that is hours behind still loses to a free one across
 * the network. A hard "always go where the data is" would idle the cluster
 * waiting for one machine.
 */
public class NearestData extends EarliestFinish {

    /** How much of the transfer a local node saves. */
    private final double transferCost;

    public NearestData() {
        this(1.0);
    }

    /**
     * @param transferCost what fetching the inputs costs, in the same units as
     *        task cost. Zero makes this behave exactly like EarliestFinish.
     */
    public NearestData(double transferCost) {
        if (transferCost < 0) {
            throw new IllegalArgumentException("transferCost must not be negative: "
                    + transferCost);
        }
        this.transferCost = transferCost;
    }

    @Override
    public String name() {
        return "NearestData";
    }

    @Override
    protected double finishTimeOn(ReadyTask task, ClusterState cluster, String node) {
        double fetch = task.inputLocations().isEmpty() || task.inputLocations().contains(node)
                ? 0
                : transferCost;
        return super.finishTimeOn(task, cluster, node) + fetch;
    }

    @Override
    public String description() {
        return "Assigns each task to the node with the earliest finish time, "
                + "counting the cost of fetching inputs it does not already hold";
    }
}
