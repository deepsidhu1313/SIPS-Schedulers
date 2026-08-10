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
 * Puts a task on whichever node is free soonest.
 *
 * <p>The obvious thing, and the baseline worth beating: it is what most systems
 * do, it needs no estimates at all, and on a cluster of identical machines it is
 * exactly right. It goes wrong only when nodes differ — then "free first" and
 * "done first" stop being the same node, which is the gap {@link EarliestFinish}
 * exists to close.
 *
 * <p>Kept because a policy that cannot beat this one is not worth its estimates.
 */
public class LeastLoaded implements PlacementPolicy {

    @Override
    public String name() {
        return "LeastLoaded";
    }

    @Override
    public Optional<String> place(ReadyTask task, ClusterState cluster) {
        String idlest = null;
        double earliest = Double.MAX_VALUE;
        for (String node : cluster.nodes()) {
            if (cluster.availableAt(node) < earliest) {
                earliest = cluster.availableAt(node);
                idlest = node;
            }
        }
        return Optional.ofNullable(idlest);
    }

    @Override
    public String description() {
        return "Assigns each task to whichever node frees up first, ignoring how fast it is";
    }
}
