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
import in.co.s13.sips.schedulers.lib.Processor;
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
public class GA2 implements Scheduler {

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

        double totalCPUScore = 0;

        /**
         * * Calculate slots available****
         */
        int availSlots = 0;
        ConcurrentHashMap<String, Processor> processors = new ConcurrentHashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            Node get = nodes.get(i);
            availSlots += (get.getTask_limit() - get.getWaiting_in_que());
            int availSlotsOnNode = (get.getTask_limit() - get.getWaiting_in_que());
            totalCPUScore += get.getCPUScore() * availSlotsOnNode;
            processors.put(get.getUuid(), new Processor(get.getUuid(), get.getCPUScore(), get.getTask_limit(), get.getWaiting_in_que()));
        }
        if (availSlots < maxNodes) {
            availSlots = maxNodes;
        }
        System.out.println("Total Score: " + totalCPUScore);
        String chunksize, lower, upper;
        boolean reverseloop = loop.isReverse();
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte = 0, chunkFactor_byte, last_up_byte = 0;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short, chunkFactor_short, last_up_short = 0;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int, chunkFactor_int, last_up_int = 0;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long, chunkFactor_long, last_up_long = 0;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float, chunkFactor_float, last_up_float = 0;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double, chunkFactor_double, last_up_double = 0;

        switch (loop.getDataType()) {
            case 0:
                min_byte = (byte) loop.getInit();
                max_byte = (byte) loop.getLimit();
                diff_byte = (byte) loop.getDiff();
                break;
            case 1:
                min_short = (short) loop.getInit();
                max_short = (short) loop.getLimit();
                diff_short = (short) loop.getDiff();
                break;
            case 2:
                min_int = (int) loop.getInit();
                max_int = (int) loop.getLimit();
                diff_int = (int) loop.getDiff();
                break;
            case 3:
                min_long = (long) loop.getInit();
                max_long = (long) loop.getLimit();
                diff_long = (long) loop.getDiff();
                break;
            case 4:
                min_float = (float) loop.getInit();
                max_float = (float) loop.getLimit();
                diff_float = (float) loop.getDiff();
                break;
            case 5:
                min_double = (double) loop.getInit();
                max_double = (double) loop.getLimit();
                diff_double = (double) loop.getDiff();
                break;
        }

        ArrayList<ParallelForSENP> elements = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            Node get = nodes.get(i);
            int availSlotsOnNode = (get.getTask_limit() - get.getWaiting_in_que());
            for (int j = 0; j < availSlotsOnNode; j++) {

                switch (loop.getDataType()) {
                    case 0:
                        chunkFactor_byte = (byte) ((byte) (get.getCPUScore() / totalCPUScore) * (diff_byte));
                        if (reverseloop) {
                            if (last_up_byte <= max_byte) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_byte = (byte) (min_byte - last_up_byte);
                            up_byte = (byte) (low_byte - chunkFactor_byte);
                            if (j == 0) {
                                up_byte--;
                            }
                            if (up_byte < max_byte || j == availSlotsOnNode - 1) {
                                up_byte = max_byte;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_byte >= max_byte) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_byte = (byte) (min_byte + last_up_byte);
                            up_byte = (byte) ((byte) (low_byte + chunkFactor_byte));
                            if (j == 0) {
                                up_byte++;
                            }
                            if (up_byte > max_byte || j == availSlotsOnNode - 1) {
                                up_byte = max_byte;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_byte = up_byte;
                        lower = "" + low_byte;
                        upper = "" + up_byte;
                        chunksize = "" + chunkFactor_byte;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                    case 1:
                        chunkFactor_short = (short) ((short) (get.getCPUScore() / totalCPUScore) * (diff_short));
                        if (reverseloop) {
                            if (last_up_short <= max_short) {
                                j = availSlotsOnNode;
                                break;
                            }

                            low_short = (short) (min_short - last_up_short);
                            up_short = (short) (low_short - chunkFactor_short);
                            if (j == 0) {
                                up_short--;
                            }
                            if (up_short < max_short || j == availSlotsOnNode - 1) {
                                up_short = max_short;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_short >= max_short) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_short = (short) (min_short + last_up_short);
                            up_short = (short) ((short) (low_short + chunkFactor_short));
                            if (j == 0) {
                                up_short++;
                            }
                            if (up_short > max_short || j == availSlotsOnNode - 1) {
                                up_short = max_short;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_short = up_short;
                        lower = "" + low_short;
                        upper = "" + up_short;
                        chunksize = "" + chunkFactor_short;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                    case 2:
                        chunkFactor_int = (int) ((get.getCPUScore() / totalCPUScore) * (diff_int));
                        if (reverseloop) {
                            if (last_up_int <= max_int) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_int = (int) (min_int - last_up_int);
                            up_int = (int) (low_int - chunkFactor_int);
                            if (j == 0) {
                                up_int--;
                            }
                            if (up_int < max_int || j == availSlotsOnNode - 1) {
                                up_int = max_int;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_int >= max_int) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_int = (int) (min_int + last_up_int);
                            up_int = (int) ((int) (low_int + chunkFactor_int));
                            if (j == 0) {
                                up_int++;
                            }
                            if (up_int > max_int || j == availSlotsOnNode - 1) {
                                up_int = max_int;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_int = up_int;
                        lower = "" + low_int;
                        upper = "" + up_int;
                        chunksize = "" + chunkFactor_int;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                    case 3:
                        chunkFactor_long = (long) ((get.getCPUScore() / totalCPUScore) * (diff_long));
                        if (reverseloop) {
                            if (last_up_long <= max_long) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_long = (long) (min_long - last_up_long);
                            up_long = (long) (low_long - chunkFactor_long);
                            if (j == 0) {
                                up_long--;
                            }
                            if (up_long < max_long || j == availSlotsOnNode - 1) {
                                up_long = max_long;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_long >= max_long) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_long = (long) (min_long + last_up_long);
                            up_long = (long) ((long) (low_long + chunkFactor_long));
                            if (j == 0) {
                                up_long++;
                            }
                            if (up_long > max_long || j == availSlotsOnNode - 1) {
                                up_long = max_long;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_long = up_long;
                        lower = "" + low_long;
                        upper = "" + up_long;
                        chunksize = "" + chunkFactor_long;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                    case 4:
                        chunkFactor_float = (float) ((get.getCPUScore() / totalCPUScore) * (diff_float));
                        if (reverseloop) {
                            if (last_up_float <= max_float) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_float = (float) (min_float - last_up_float);
                            up_float = (float) (low_float - chunkFactor_float);
                            if (j == 0) {
                                up_float--;
                            }
                            if (up_float < max_float || j == availSlotsOnNode - 1) {
                                up_float = max_float;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_float >= max_float) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_float = (float) (min_float + last_up_float);
                            up_float = (float) ((float) (low_float + chunkFactor_float));
                            if (j == 0) {
                                up_float++;
                            }
                            if (up_float > max_float || j == availSlotsOnNode - 1) {
                                up_float = max_float;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_float = up_float;
                        lower = "" + low_float;
                        upper = "" + up_float;
                        chunksize = "" + chunkFactor_float;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                    case 5:
                        chunkFactor_double = (double) ((get.getCPUScore() / totalCPUScore) * (diff_double));
                        if (reverseloop) {
                            if (last_up_double <= max_double) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_double = (double) (min_double - last_up_double);
                            up_double = (double) (low_double - chunkFactor_double);
                            if (j == 0) {
                                up_double--;
                            }
                            if (up_double < max_double || j == availSlotsOnNode - 1) {
                                up_double = max_double;
                                j = availSlotsOnNode;
                            }
                        } else {
                            if (last_up_double >= max_double) {
                                j = availSlotsOnNode;
                                break;
                            }
                            low_double = (double) (min_double + last_up_double);
                            up_double = (double) ((double) (low_double + chunkFactor_double));
                            if (j == 0) {
                                up_double++;
                            }
                            if (up_double > max_double || j == availSlotsOnNode - 1) {
                                up_double = max_double;
                                j = availSlotsOnNode;
                            }
                        }
                        last_up_double = up_double;
                        lower = "" + low_double;
                        upper = "" + up_double;
                        chunksize = "" + chunkFactor_double;
                        elements.add(new ParallelForSENP(lower, upper, get.getUuid(), chunksize));
                        break;
                }

            }
        }
        Chromosome<ParallelForSENP> bestPossibleChromosome = new Chromosome(elements);
        Chromosome<ParallelForSENP> bestChromosome = new Chromosome(elements);
        Collections.sort(nodes, (LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));

        ArrayList<Chromosome> chromosomes = new ArrayList<>();
        for (int i = 0; i < maxPopulation; i++) {
            ArrayList<ParallelForSENP> al = bestPossibleChromosome.getElements();
            for (int j = 0; j < al.size(); j++) {
                ParallelForSENP get = al.get(j);
                ParallelForSENP newParallelForSENP = new ParallelForSENP(get);
                Processor processor = processors.get(nodes.get((int) (nodes.indexOf(liveNodes.get(get.getNodeUUID())) * Math.random())));

            }
        }
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
                    randomCrossOverPoint = ((bestChromosome.getScheduleLength()) / 2);
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
            bestChromosome.getElements().get((int) (Math.random() * (bestChromosome.getElements().size() - 1))).setNodeUUID(nodes.get(0).getUuid());
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

    private class Chromosome<T> {

        private ArrayList<T> elements = new ArrayList<>();

        public Chromosome() {

        }

        public Chromosome(ArrayList<T> elements) {
            this.elements = elements;
        }

        public ArrayList<T> getElements() {
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
