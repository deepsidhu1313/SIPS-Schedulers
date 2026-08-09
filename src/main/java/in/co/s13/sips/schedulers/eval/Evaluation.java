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

/**
 * What a policy achieved on a workload.
 *
 * @param policy        the scheduling policy
 * @param workload      the workload it ran
 * @param nodes         how many nodes were available
 * @param makespan      when the last node finished; the number that matters
 * @param serialCost    total work, i.e. the time one node would take
 * @param busiest       time spent by the node that finished last
 * @param idlest        time spent by the node that finished first
 */
public record Evaluation(String policy, String workload, int nodes,
        double makespan, double serialCost, double busiest, double idlest) {

    /** How much faster than a single node. */
    public double speedup() {
        return makespan <= 0 ? 0 : serialCost / makespan;
    }

    /** Speedup per node. 1.0 is perfect; anything less is wasted capacity. */
    public double efficiency() {
        return nodes <= 0 ? 0 : speedup() / nodes;
    }

    /**
     * Spread between the busiest and idlest node, as a fraction of the busiest.
     *
     * <p>Zero is perfectly balanced. This is what a scheduling policy is
     * actually trying to minimise, and it explains a poor makespan when the
     * total work was fine.
     */
    public double loadImbalance() {
        return busiest <= 0 ? 0 : (busiest - idlest) / busiest;
    }

    @Override
    public String toString() {
        return String.format("%-10s makespan %8.1f  speedup %5.2fx  efficiency %4.0f%%  imbalance %4.0f%%",
                policy, makespan, speedup(), efficiency() * 100, loadImbalance() * 100);
    }
}
