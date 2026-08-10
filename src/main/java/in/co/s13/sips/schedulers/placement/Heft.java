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

import in.co.s13.sips.scheduler.ReadyTask;
import java.util.Comparator;
import java.util.List;

/**
 * Heterogeneous Earliest Finish Time (Topcuoglu, Hariri and Wu, 2002).
 *
 * <p>Two rules, and both are needed:
 *
 * <ol>
 *   <li>consider the task with the most work still ahead of it first — its
 *       upward rank;</li>
 *   <li>give it to the node that would finish it soonest.</li>
 * </ol>
 *
 * <p>Rule 2 alone is {@link EarliestFinish}, and on its own it will happily
 * start a cheap leaf while the stage that gates a long chain waits for a node.
 * Rule 1 is what makes the difference on any graph whose branches are uneven,
 * which is most real pipelines.
 *
 * <p>The reference implementation for {@link in.co.s13.sips.scheduler.PlacementPolicy}:
 * the thing a new heuristic gets compared against.
 *
 * <h2>What it does not do</h2>
 *
 * <p><b>Insertion-less.</b> The original algorithm may slot a task into an idle
 * gap earlier in a node's schedule; this does not. It only ever appends. On a
 * cluster where one node is much faster, that piles a queue onto it while
 * another sits idle, and a policy as naive as {@link LeastLoaded} can win — the
 * evaluator suite has a case that measures exactly this. Adding insertion is the
 * obvious next improvement, and the reason it is worth having the evaluator
 * before making it.
 *
 * <p><b>No transfer cost.</b> HEFT's published form counts the time to move a
 * task's inputs to the node considered. That needs to know where the inputs are,
 * which is what {@link NearestData} models today.
 */
public class Heft extends EarliestFinish {

    @Override
    public String name() {
        return "HEFT";
    }

    @Override
    public List<ReadyTask> order(List<ReadyTask> ready) {
        // Most critical first. Name breaks ties so two runs of the same graph
        // schedule identically -- a heuristic nobody can reproduce is not
        // evidence.
        return ready.stream()
                .sorted(Comparator.comparingDouble(ReadyTask::upwardRank).reversed()
                        .thenComparing(ReadyTask::name))
                .toList();
    }

    @Override
    public String description() {
        return "Orders ready tasks by upward rank, then assigns each to the node "
                + "with the earliest estimated finish time";
    }
}
