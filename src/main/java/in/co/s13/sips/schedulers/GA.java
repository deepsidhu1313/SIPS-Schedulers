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
import org.json.JSONObject;

/**
 *
 * @author nika
 */
public class GA implements Scheduler {

    @Override
    public ArrayList<TaskNodePair> schedule() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ArrayList<Node> nodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        System.out.println("Before Sorting:" + nodes);

        /**
         ** Selection
         */
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        System.out.println("After Sorting:" + nodes);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);
        int maxGenerations = schedulerSettings.getInt("MaxGenerations", 4);
        if (maxNodes < nodes.size()) {
            // select best nodes for scheduling
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }
        Collections.sort(nodes, LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed());

        double maxCPUScore = nodes.get(0).getCPUScore();
        double minCPUScore = nodes.get(nodes.size() - 1).getCPUScore();

        int totalnodes = nodes.size();

        /**
         * * Calculate slots available****
         */
        int availSlots = 0;
        double minExpectedTime = 1;
        for (int i = 0; i < nodes.size(); i++) {
            Node get = nodes.get(i);
            availSlots += (get.getTask_limit() - get.getWaiting_in_que());
            minExpectedTime = ((minExpectedTime) * (maxCPUScore / get.getCPUScore())) / ((minExpectedTime) + (maxCPUScore / get.getCPUScore()));
        }
        if (availSlots < maxNodes) {
            availSlots = maxNodes;
        }

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

        ArrayList<ParallelForSENP> elements = new ArrayList<>();
        for (int j = 0; j < nodes.size(); j++) {
            Node get = nodes.get(j);

            switch (loop.getDataType()) {
                case 0:
                    chunkFactor_byte = (byte) ((byte) (get.getCPUScore() / maxCPUScore) * (max_cs_byte));
                    if (reverseloop) {
                        low_byte = (byte) (min_byte - last_up_byte);
                        up_byte = (byte) (low_byte - chunkFactor_byte);
                    } else {
                        low_byte = (byte) (min_byte + last_up_byte);
                        up_byte = (byte) ((byte) (low_byte + chunkFactor_byte));
                    }
                    last_up_byte = up_byte;
                    lower = "" + low_byte;
                    upper = "" + up_byte;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
                case 1:
                    chunkFactor_short = (short) ((short) (get.getCPUScore() / maxCPUScore) * (max_cs_short));
                    if (reverseloop) {
                        low_short = (short) (min_short - last_up_short);
                        up_short = (short) (low_short - chunkFactor_short);
                    } else {
                        low_short = (short) (min_short + last_up_short);
                        up_short = (short) ((short) (low_short + chunkFactor_short));
                    }
                    last_up_short = up_short;
                    lower = "" + low_short;
                    upper = "" + up_short;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
                case 2:
                    chunkFactor_int = (int) ((get.getCPUScore() / maxCPUScore) * (max_cs_int));
                    if (reverseloop) {
                        low_int = (int) (min_int - last_up_int);
                        up_int = (int) (low_int - chunkFactor_int);
                    } else {
                        low_int = (int) (min_int + last_up_int);
                        up_int = (int) ((int) (low_int + chunkFactor_int));
                    }
                    last_up_int = up_int;
                    lower = "" + low_int;
                    upper = "" + up_int;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
                case 3:
                    chunkFactor_long = (long) ((get.getCPUScore() / maxCPUScore) * (max_cs_long));
                    if (reverseloop) {
                        low_long = (long) (min_long - last_up_long);
                        up_long = (long) (low_long - chunkFactor_long);
                    } else {
                        low_long = (long) (min_long + last_up_long);
                        up_long = (long) ((long) (low_long + chunkFactor_long));
                    }
                    last_up_long = up_long;
                    lower = "" + low_long;
                    upper = "" + up_long;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
                case 4:
                    chunkFactor_float = (float) ((get.getCPUScore() / maxCPUScore) * (max_cs_float));
                    if (reverseloop) {
                        low_float = (float) (min_float - last_up_float);
                        up_float = (float) (low_float - chunkFactor_float);
                    } else {
                        low_float = (float) (min_float + last_up_float);
                        up_float = (float) ((float) (low_float + chunkFactor_float));
                    }
                    last_up_float = up_float;
                    lower = "" + low_float;
                    upper = "" + up_float;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
                case 5:
                    chunkFactor_double = (double) ((get.getCPUScore() / maxCPUScore) * (max_cs_double));
                    if (reverseloop) {
                        low_double = (double) (min_double - last_up_double);
                        up_double = (double) (low_double - chunkFactor_double);
                    } else {
                        low_double = (double) (min_double + last_up_double);
                        up_double = (double) ((double) (low_double + chunkFactor_double));
                    }
                    last_up_double = up_double;
                    lower = "" + low_double;
                    upper = "" + up_double;
                    elements.add(new ParallelForSENP(lower, upper, get.getUuid()));
                    break;
            }

        }
        System.out.println("Elements:"+elements);

        return result;
    }

    private class Chromosome {

        private ArrayList<ParallelForSENP> elements = new ArrayList<>();
        private ArrayList<Node> nodes = new ArrayList<>();
        private double scheduleLength;

        public Chromosome() {

        }

        public void addElement(ParallelForSENP element) {
            elements.add(element);
        }

        public ArrayList<ParallelForSENP> getElements() {
            return elements;
        }

        public double getScheduleLength() {
            return scheduleLength;
        }

    }

}
