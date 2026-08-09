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
import java.util.List;
import java.util.Random;

/**
 * A described workload: how many chunks, and what each costs.
 *
 * <p>Cost is in arbitrary time units. What matters for comparing policies is
 * the <em>shape</em> of the distribution, not the absolute numbers.
 *
 * <p>Deterministic by construction — a fixed seed derived from the name — because
 * a benchmark nobody can reproduce is not evidence.
 */
public final class Workload {

    private final String name;
    private final List<Double> costs;

    private Workload(String name, List<Double> costs) {
        this.name = name;
        this.costs = List.copyOf(costs);
    }

    /** Every chunk costs the same. The easy case; most policies tie. */
    public static Workload uniform(String name, int chunks, double cost) {
        require(chunks > 0, "Chunk count must be positive: " + chunks);
        require(cost > 0, "Cost must be positive: " + cost);
        List<Double> costs = new ArrayList<>();
        for (int i = 0; i < chunks; i++) {
            costs.add(cost);
        }
        return new Workload(name, costs);
    }

    /**
     * Cost varies between a floor and a ceiling.
     *
     * <p>This is the case that separates policies. Mandelbrot is the canonical
     * example: a point inside the set iterates to the limit while one outside
     * escapes almost at once, so neighbouring chunks can differ by two orders
     * of magnitude. Static assignment cannot recover from a bad split; a
     * self-scheduling policy hands out small pieces near the end and keeps
     * every node busy.
     */
    public static Workload skewed(String name, int chunks, double min, double max) {
        require(chunks > 0, "Chunk count must be positive: " + chunks);
        require(min > 0, "Minimum cost must be positive: " + min);
        require(max >= min, "Maximum cost " + max + " is below minimum " + min);

        Random random = new Random(name.hashCode());
        List<Double> costs = new ArrayList<>();
        for (int i = 0; i < chunks; i++) {
            // Squared so most chunks are cheap and a few are very expensive,
            // which is the shape irregular problems actually have.
            double t = random.nextDouble();
            costs.add(min + (max - min) * t * t);
        }
        return new Workload(name, costs);
    }

    /** A workload with costs given directly, for replaying a real measurement. */
    public static Workload measured(String name, List<Double> costs) {
        require(costs != null && !costs.isEmpty(), "At least one chunk is required");
        return new Workload(name, costs);
    }

    public String name() {
        return name;
    }

    public int chunkCount() {
        return costs.size();
    }

    public double costOf(int chunk) {
        return costs.get(chunk);
    }

    public double totalCost() {
        return costs.stream().mapToDouble(Double::doubleValue).sum();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
