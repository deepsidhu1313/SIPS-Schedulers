/*
 * Copyright (C) 2018 Navdeep Singh Sidhu
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

import in.co.s13.sips.lib.ParallelForSENP;
import in.co.s13.sips.lib.TaskNodePair;
import in.co.s13.sips.lib.common.datastructure.LiveNode;
import in.co.s13.sips.lib.common.datastructure.Node;
import in.co.s13.sips.lib.common.datastructure.ParallelForLoop;
import in.co.s13.sips.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

/**
 *
 * @author nika
 */
public class GA implements Scheduler {

    private int nodes;
    private ArrayList<Node> backupNodes = new ArrayList<>();

    @Override
    public ArrayList<TaskNodePair> schedule() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(liveNodes.values());
        System.out.println("Before Sorting:" + nodes);

        /**
         ** Selection
         */
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        System.out.println("After Sorting:" + nodes);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);
        int maxGenerations = schedulerSettings.getInt("MaxGenerations", 4);
        int maxPopulation = schedulerSettings.getInt("MaxPopulation", 8);
        if (maxNodes > 8) {
            Node node = liveNodes.get(in.co.s13.sips.lib.node.settings.GlobalValues.NODE_UUID);
            nodes.remove(node);
        }
        if (maxNodes < nodes.size()) {
            // select best nodes for scheduling
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }
        Collections.sort(nodes, LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed());

        Node bestNode = nodes.get(0);
        double maxCPUScore = bestNode.getCPUScore();

        int totalnodes = nodes.size();

        /**
         * * Calculate slots available****
         */
        int availSlots = nodes.get(0).getTask_limit() - nodes.get(0).getWaiting_in_que();
        double minExpectedTime = 1;
        for (int i = 1; i < nodes.size(); i++) {
            Node get = nodes.get(i);
            availSlots += (get.getTask_limit() - get.getWaiting_in_que());
            int availSlotsOnNode = (get.getTask_limit() - get.getWaiting_in_que());
            for (int j = 0; j < availSlotsOnNode; j++) {
                minExpectedTime = ((minExpectedTime) * (maxCPUScore / get.getCPUScore())) / ((minExpectedTime) + (maxCPUScore / get.getCPUScore()));
            }
        }
        if (availSlots < maxNodes) {
            availSlots = maxNodes;
        }
        System.out.println("Max Score: " + maxCPUScore + " Min Expected Time:" + minExpectedTime);
        String chunksize, lower, upper;
        boolean reverseloop = loop.isReverse();
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte = 0, chunkFactor_byte, last_up_byte = 0, max_cs_byte = 0;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short, chunkFactor_short, last_up_short = 0, max_cs_short = 0;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int, chunkFactor_int, last_up_int = 0, max_cs_int = 0;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long, chunkFactor_long, last_up_long = 0, max_cs_long = 0;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float, chunkFactor_float, last_up_float = 0, max_cs_float = 0;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double, chunkFactor_double, last_up_double = 0, max_cs_double = 0;

        switch (loop.getDataType()) {
            case 0:
                min_byte = (byte) loop.getInit();
                max_byte = (byte) loop.getLimit();
                diff_byte = (byte) loop.getDiff();
                max_cs_byte = (byte) (minExpectedTime * diff_byte);
                break;
            case 1:
                min_short = (short) loop.getInit();
                max_short = (short) loop.getLimit();
                diff_short = (short) loop.getDiff();
                max_cs_short = (short) (minExpectedTime * diff_short);
                break;
            case 2:
                min_int = (int) loop.getInit();
                max_int = (int) loop.getLimit();
                diff_int = (int) loop.getDiff();
                max_cs_int = (int) (minExpectedTime * diff_int);
                break;
            case 3:
                min_long = (long) loop.getInit();
                max_long = (long) loop.getLimit();
                diff_long = (long) loop.getDiff();
                max_cs_long = (long) (minExpectedTime * diff_long);
                break;
            case 4:
                min_float = (float) loop.getInit();
                max_float = (float) loop.getLimit();
                diff_float = (float) loop.getDiff();
                max_cs_float = (float) (minExpectedTime * diff_float);
                break;
            case 5:
                min_double = (double) loop.getInit();
                max_double = (double) loop.getLimit();
                diff_double = (double) loop.getDiff();
                max_cs_double = (double) (minExpectedTime * diff_double);
                break;
        }

        ArrayList<LiveNode.LiveNodeComparator> comprators = new ArrayList<>();
        Collections.addAll(comprators, LiveNode.LiveNodeComparator.values());
        ArrayList<Chromosome> chromosomes = new ArrayList<>();
        for (int i = 0; i < maxPopulation; i++) {
            low_byte = 0;
            up_byte = 0;
            chunkFactor_byte = 0;
            last_up_byte = 0;
            low_short = 0;
            up_short = 0;
            chunkFactor_short = 0;
            last_up_short = 0;
            low_int = 0;
            up_int = 0;
            chunkFactor_int = 0;
            last_up_int = 0;
            low_long = 0;
            up_long = 0;
            chunkFactor_long = 0;
            last_up_long = 0;
            low_float = 0;
            up_float = 0;
            chunkFactor_float = 0;
            last_up_float = 0;
            low_double = 0;
            up_double = 0;
            chunkFactor_double = 0;
            last_up_double = 0;

            LiveNode.LiveNodeComparator randomComparator = comprators.get((int) (Math.random() * (comprators.size() - 1)));
            if (Math.random() < 0.5) {
                Collections.sort(nodes, randomComparator.reversed());
            } else {
                Collections.sort(nodes, randomComparator);
            }
            ArrayList<ParallelForSENP> elements = new ArrayList<>();
            for (int j = 0; j < availSlots; j++) {
                Node randomlySelectedNode = nodes.get((int) (Math.random() * (nodes.size() - 1)));

                switch (loop.getDataType()) {
                    case 0:
                        chunkFactor_byte = (byte) ((byte) (randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_byte));
                        if (reverseloop) {
                            if (last_up_byte <= max_byte) {
                                j = availSlots;
                                break;
                            }
                            low_byte = (byte) (min_byte - last_up_byte);
                            up_byte = (byte) (low_byte - chunkFactor_byte);
                            if (j == 0) {
                                up_byte--;
                            }
                            if (up_byte < max_byte || j == availSlots - 1) {
                                up_byte = max_byte;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_byte >= max_byte) {
                                j = availSlots;
                                break;
                            }
                            low_byte = (byte) (min_byte + last_up_byte);
                            up_byte = (byte) ((byte) (low_byte + chunkFactor_byte));
                            if (j == 0) {
                                up_byte++;
                            }
                            if (up_byte > max_byte || j == availSlots - 1) {
                                up_byte = max_byte;
                                j = availSlots;
                            }
                        }
                        last_up_byte = up_byte;
                        lower = "" + low_byte;
                        upper = "" + up_byte;
                        chunksize = "" + chunkFactor_byte;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                    case 1:
                        chunkFactor_short = (short) ((short) (randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_short));
                        if (reverseloop) {
                            if (last_up_short <= max_short) {
                                j = availSlots;
                                break;
                            }

                            low_short = (short) (min_short - last_up_short);
                            up_short = (short) (low_short - chunkFactor_short);
                            if (j == 0) {
                                up_short--;
                            }
                            if (up_short < max_short || j == availSlots - 1) {
                                up_short = max_short;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_short >= max_short) {
                                j = availSlots;
                                break;
                            }
                            low_short = (short) (min_short + last_up_short);
                            up_short = (short) ((short) (low_short + chunkFactor_short));
                            if (j == 0) {
                                up_short++;
                            }
                            if (up_short > max_short || j == availSlots - 1) {
                                up_short = max_short;
                                j = availSlots;
                            }
                        }
                        last_up_short = up_short;
                        lower = "" + low_short;
                        upper = "" + up_short;
                        chunksize = "" + chunkFactor_short;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                    case 2:
                        chunkFactor_int = (int) ((randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_int));
                        if (reverseloop) {
                            if (last_up_int <= max_int) {
                                j = availSlots;
                                break;
                            }
                            low_int = (int) (min_int - last_up_int);
                            up_int = (int) (low_int - chunkFactor_int);
                            if (j == 0) {
                                up_int--;
                            }
                            if (up_int < max_int || j == availSlots - 1) {
                                up_int = max_int;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_int >= max_int) {
                                j = availSlots;
                                break;
                            }
                            low_int = (int) (min_int + last_up_int);
                            up_int = (int) ((int) (low_int + chunkFactor_int));
                            if (j == 0) {
                                up_int++;
                            }
                            if (up_int > max_int || j == availSlots - 1) {
                                up_int = max_int;
                                j = availSlots;
                            }
                        }
                        last_up_int = up_int;
                        lower = "" + low_int;
                        upper = "" + up_int;
                        chunksize = "" + chunkFactor_int;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                    case 3:
                        chunkFactor_long = (long) ((randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_long));
                        if (reverseloop) {
                            if (last_up_long <= max_long) {
                                j = availSlots;
                                break;
                            }
                            low_long = (long) (min_long - last_up_long);
                            up_long = (long) (low_long - chunkFactor_long);
                            if (j == 0) {
                                up_long--;
                            }
                            if (up_long < max_long || j == availSlots - 1) {
                                up_long = max_long;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_long >= max_long) {
                                j = availSlots;
                                break;
                            }
                            low_long = (long) (min_long + last_up_long);
                            up_long = (long) ((long) (low_long + chunkFactor_long));
                            if (j == 0) {
                                up_long++;
                            }
                            if (up_long > max_long || j == availSlots - 1) {
                                up_long = max_long;
                                j = availSlots;
                            }
                        }
                        last_up_long = up_long;
                        lower = "" + low_long;
                        upper = "" + up_long;
                        chunksize = "" + chunkFactor_long;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                    case 4:
                        chunkFactor_float = (float) ((randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_float));
                        if (reverseloop) {
                            if (last_up_float <= max_float) {
                                j = availSlots;
                                break;
                            }
                            low_float = (float) (min_float - last_up_float);
                            up_float = (float) (low_float - chunkFactor_float);
                            if (j == 0) {
                                up_float--;
                            }
                            if (up_float < max_float || j == availSlots - 1) {
                                up_float = max_float;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_float >= max_float) {
                                j = availSlots;
                                break;
                            }
                            low_float = (float) (min_float + last_up_float);
                            up_float = (float) ((float) (low_float + chunkFactor_float));
                            if (j == 0) {
                                up_float++;
                            }
                            if (up_float > max_float || j == availSlots - 1) {
                                up_float = max_float;
                                j = availSlots;
                            }
                        }
                        last_up_float = up_float;
                        lower = "" + low_float;
                        upper = "" + up_float;
                        chunksize = "" + chunkFactor_float;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                    case 5:
                        chunkFactor_double = (double) ((randomlySelectedNode.getCPUScore() / maxCPUScore) * (max_cs_double));
                        if (reverseloop) {
                            if (last_up_double <= max_double) {
                                j = availSlots;
                                break;
                            }
                            low_double = (double) (min_double - last_up_double);
                            up_double = (double) (low_double - chunkFactor_double);
                            if (j == 0) {
                                up_double--;
                            }
                            if (up_double < max_double || j == availSlots - 1) {
                                up_double = max_double;
                                j = availSlots;
                            }
                        } else {
                            if (last_up_double >= max_double) {
                                j = availSlots;
                                break;
                            }
                            low_double = (double) (min_double + last_up_double);
                            up_double = (double) ((double) (low_double + chunkFactor_double));
                            if (j == 0) {
                                up_double++;
                            }
                            if (up_double > max_double || j == availSlots - 1) {
                                up_double = max_double;
                                j = availSlots;
                            }
                        }
                        last_up_double = up_double;
                        lower = "" + low_double;
                        upper = "" + up_double;
                        chunksize = "" + chunkFactor_double;
                        elements.add(new ParallelForSENP(lower, upper, randomlySelectedNode.getUuid(), chunksize));
                        break;
                }

            }
            System.out.println("Elements:" + elements);
            chromosomes.add(new Chromosome(elements));
        }
        Chromosome bestChromosome = chromosomes.get((int) (Math.random() * (chromosomes.size() - 1)));
        for (int i = 0; i < maxGenerations; i++) {
            /**
             * *CrossOver**
             */
            boolean failedToChoosePoint = false;
            int counter = 0;
            Chromosome randomChromosome = chromosomes.get((int) (Math.random() * (chromosomes.size() - 1)));
            if (bestChromosome.getScheduleLength() == randomChromosome.getScheduleLength() && bestChromosome.getScheduleLength() > 2) {
                int randomCrossOverPoint = (int) (Math.random() * (bestChromosome.getScheduleLength() - 1));
                while (randomCrossOverPoint >= (bestChromosome.getScheduleLength() - 1)) {
                    randomCrossOverPoint = (int) (Math.random() * (bestChromosome.getScheduleLength() - 1));

                    if (counter > 10) {
                        failedToChoosePoint = true;
                    }
                    counter++;
                }
                if (failedToChoosePoint) {
                    randomCrossOverPoint = ((bestChromosome.getScheduleLength() - 1) / 2);
                }
                System.out.println("Crossover at :" + randomCrossOverPoint + " on list of size: " + bestChromosome.getScheduleLength());
                List<ParallelForSENP> first = bestChromosome.getElements().subList(0, randomCrossOverPoint);
                System.out.println("Sublist 1:" + first);
                List<ParallelForSENP> second = bestChromosome.getElements().subList(randomCrossOverPoint, bestChromosome.getScheduleLength());
                System.out.println("Sublist 2:" + second);
                List<ParallelForSENP> third = randomChromosome.getElements().subList(0, randomCrossOverPoint);
                System.out.println("Sublist 3:" + third);
                List<ParallelForSENP> fourth = randomChromosome.getElements().subList(randomCrossOverPoint, randomChromosome.getScheduleLength());
                System.out.println("Sublist 4:" + fourth);
                ArrayList<ParallelForSENP> temp1 = new ArrayList<>();
                temp1.addAll(first);
                temp1.addAll(fourth);
                ArrayList<ParallelForSENP> temp2 = new ArrayList<>();
                temp2.addAll(third);
                temp2.addAll(second);
                bestChromosome = new Chromosome(temp1);
                randomChromosome = new Chromosome(temp2);
            }

            /**
             * *Mutation**
             */
            bestChromosome.getElements().get((int) (Math.random() * (bestChromosome.getElements().size() - 1))).setNodeUUID(bestNode.getUuid());
        }
        System.out.println("Best Chromosome:" + bestChromosome.getElements());
        this.nodes = bestChromosome.getElements().size();
        backupNodes.addAll(nodes);

        return bestChromosome.getElements();
    }

    @Override
    public int getTotalNodes() {
        return this.nodes;
    }

    @Override
    public ArrayList<Node> getBackupNodes() {
        return backupNodes;
    }

    @Override
    public int getTotalChunks() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class Chromosome {

        private ArrayList<ParallelForSENP> elements = new ArrayList<>();

        public Chromosome(ArrayList<ParallelForSENP> elements) {
            this.elements = elements;
        }

        public ArrayList<ParallelForSENP> getElements() {
            return elements;
        }

        public int getScheduleLength() {
            return elements.size();
        }

    }

    public static enum ChromosomeComparator implements Comparator<Chromosome> {

        SCHEDULE_LENGTH_SORT {
            public int compare(Chromosome o1, Chromosome o2) {
                return Integer.valueOf(o1.getScheduleLength()).compareTo(o2.getScheduleLength());
            }
        };

        public static Comparator<Chromosome> decending(final Comparator<Chromosome> other) {
            return new Comparator<Chromosome>() {
                public int compare(Chromosome o1, Chromosome o2) {
                    return -1 * other.compare(o1, o2);
                }
            };
        }

        public static Comparator<Chromosome> getComparator(final ChromosomeComparator... multipleOptions) {
            return new Comparator<Chromosome>() {
                public int compare(Chromosome o1, Chromosome o2) {
                    for (ChromosomeComparator option : multipleOptions) {
                        int result = option.compare(o1, o2);
                        if (result != 0) {
                            return result;
                        }
                    }
                    return 0;
                }
            };
        }
    }

}
