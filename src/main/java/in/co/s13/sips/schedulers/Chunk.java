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

import in.co.s13.sips.lib.common.datastructure.ParallelForSENP;
import in.co.s13.sips.lib.common.datastructure.SIPSTask;
import in.co.s13.sips.lib.common.datastructure.LiveNode;
import in.co.s13.sips.lib.common.datastructure.Node;
import in.co.s13.sips.lib.common.datastructure.ParallelForLoop;
import in.co.s13.sips.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

public class Chunk implements Scheduler {

    private int nodes, totalChunks, selectedNodes;
    private ArrayList<Node> backupNodes = new ArrayList<>();
    private ArrayList<String> errors = new ArrayList<>();
    private ArrayList<String> outputs = new ArrayList<>();

    @Override
    public ArrayList<String> getErrors() {
        return errors;
    }

    @Override
    public ArrayList<String> getOutputs() {
        return outputs;
    }

    @Override
    public ArrayList<SIPSTask> schedule(ConcurrentHashMap<String, Node> livenodes, ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {
        ArrayList<SIPSTask> result = new ArrayList<>();
        outputs.add("Tasks: " + tasks.values());
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(livenodes.values());

        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);

        if (maxNodes > 1  && nodes.size() > 1) {
            Node node = livenodes.get(in.co.s13.sips.lib.node.settings.GlobalValues.NODE_UUID);
            nodes.remove(node);
        }
        if (maxNodes < nodes.size()) {
            // select best nodes for scheduling
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }

        ArrayList<SIPSTask> tasksList = sortTasksAccordingToDependencies(tasks);
//        outputs.add("UnSorted Tasks:" + tasksList);
        outputs.add("Sorted Tasks:" + tasksList);
        int nodeCounter = 0;
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            Node node = nodes.get(nodeCounter);
            get.setNodeUUID(node.getUuid());
            result.add(get);
            nodeCounter++;
            if (nodeCounter == nodes.size()) {
                nodeCounter = 0;
            }
        }

        this.nodes = nodes.size();
        this.selectedNodes = nodes.size();
        backupNodes.addAll(nodes);
        totalChunks = result.size();

        return result;
    }

    private ArrayList<SIPSTask> sortTasksAccordingToDependencies(ConcurrentHashMap<String, SIPSTask> tasks) {
        ArrayList<SIPSTask> tasksList = new ArrayList<>(tasks.values());
        outputs.add("UnSorted Tasks:" + tasksList);
        Collections.sort(tasksList, SIPSTask.SIPSTaskComparator.NO_OF_DEPENDENCIES.thenComparing(SIPSTask.SIPSTaskComparator.ID));
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            ArrayList<String> deps = get.getDependsOn();
            for (int j = 0; j < deps.size(); j++) {
                String get1 = deps.get(j);
                SIPSTask dep = tasks.get(get1);
                int depCurrentIndex = getTaskIndex(tasksList, dep);
                int tasksCurrentIndex = getTaskIndex(tasksList, get);
                if (depCurrentIndex > tasksCurrentIndex) {
                    tasksList.remove(depCurrentIndex);
                    tasksList.add(tasksCurrentIndex, dep);
                    i = 0;
                }
            }

        }

        return tasksList;
    }

    private int getTaskIndex(ArrayList<SIPSTask> tasksList, SIPSTask task) {
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            if (get.equals(task)) {
                return i;
            }
        }
        return 0;
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> livenodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(livenodes.values());

        outputs.add("Before Sorting:" + nodes);
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        outputs.add("After Sorting:" + nodes);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);

        if (maxNodes > 1 && nodes.size() > 1) {
            Node node = livenodes.get(in.co.s13.sips.lib.node.settings.GlobalValues.NODE_UUID);
            nodes.remove(node);
        }
        if (maxNodes < nodes.size()) {
            // select best nodes for scheduling
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }
        String chunksize, lower, upper;
        boolean reverseloop = loop.isReverse();
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double;

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
        int totalnodes = nodes.size();
        for (int i = 1; i <= nodes.size(); i++) {
            Node get = nodes.get(i - 1);
            switch (loop.getDataType()) {
                case 0:
                    chunksize = "" + diff_byte / totalnodes;
                    if (i == 1) {
                        low_byte = (byte) (((diff_byte / totalnodes) * (i - 1)));
                    } else {
                        low_byte = (byte) (((diff_byte / totalnodes) * (i - 1)) + 1);
                    }

                    up_byte = (byte) ((diff_byte / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_byte - low_byte);
                        if (i == totalnodes) {
                            upper = "" + ((max_byte - (min_byte - up_byte)) + (min_byte - up_byte));
                        } else {
                            upper = "" + (min_byte - up_byte);
                        }

                    } else {
                        lower = "" + (min_byte + low_byte);
                        if (i == totalnodes) {
                            upper = "" + ((max_byte - (min_byte + up_byte)) + (min_byte + up_byte));
                        } else {

                            upper = "" + (min_byte + up_byte);
                        }

                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;
                case 1:
                    chunksize = "" + diff_short / totalnodes;
                    if (i == 1) {
                        low_short = (short) (((diff_short / totalnodes) * (i - 1)));
                    } else {
                        low_short = (short) (((diff_short / totalnodes) * (i - 1)) + 1);
                    }
                    up_short = (short) ((diff_short / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_short - low_short);
                        if (i == totalnodes) {
                            upper = "" + ((max_short - (min_short - up_short)) + (min_short - up_short));
                        } else {

                            upper = "" + (min_short - up_short);
                        }

                    } else {
                        lower = "" + (min_short + low_short);
                        if (i == totalnodes) {
                            upper = "" + ((max_short - (min_short + up_short)) + (min_short + up_short));
                        } else {

                            upper = "" + (min_short + up_short);
                        }

                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;
                case 2:
                    chunksize = "" + diff_int / totalnodes;
                    if (i == 1) {
                        low_int = (((diff_int / totalnodes) * (i - 1)));
                    } else {
                        low_int = (((diff_int / totalnodes) * (i - 1)) + 1);
                    }
                    up_int = ((diff_int / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_int - low_int);
                        if (i == totalnodes) {
                            upper = "" + ((max_int - (min_int - up_int)) + (min_int - up_int));
                        } else {

                            upper = "" + (min_int - up_int);
                        }

                    } else {
                        lower = "" + (min_int + low_int);
                        if (i == totalnodes) {
                            upper = "" + ((max_int - (min_int + up_int)) + (min_int + up_int));
                        } else {

                            upper = "" + (min_int + up_int);
                        }

                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;
                case 3:

                    chunksize = "" + diff_long / totalnodes;
                    if (i == 1) {
                        low_long = (((diff_long / totalnodes) * (i - 1)));
                    } else {
                        low_long = (((diff_long / totalnodes) * (i - 1)) + 1);
                    }
                    up_long = ((diff_long / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_long - low_long);
                        if (i == totalnodes) {
                            upper = "" + ((max_long - (min_long - up_long)) + (min_long - up_long));
                        } else {

                            upper = "" + (min_long - up_long);
                        }

                    } else {
                        lower = "" + (min_long + low_long);
                        if (i == totalnodes) {
                            upper = "" + ((max_long - (min_long + up_long)) + (min_long + up_long));
                        } else {

                            upper = "" + (min_long + up_long);
                        }
                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;
                case 4:
                    chunksize = "" + diff_float / totalnodes;
                    if (i == 1) {
                        low_float = (((diff_float / totalnodes) * (i - 1)));
                    } else {
                        low_float = (((diff_float / totalnodes) * (i - 1)) + 1);
                    }
                    up_float = ((diff_float / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_float - low_float);
                        if (i == totalnodes) {
                            upper = "" + ((max_float - (min_float - up_float)) + (min_float - up_float));
                        } else {

                            upper = "" + (min_float - up_float);
                        }
                    } else {
                        lower = "" + (min_float + low_float);
                        if (i == totalnodes) {
                            upper = "" + ((max_float - (min_float + up_float)) + (min_float + up_float));
                        } else {

                            upper = "" + (min_float + up_float);
                        }

                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;
                case 5:
                    chunksize = "" + diff_double / totalnodes;
                    if (i == 1) {
                        low_double = (((diff_double / totalnodes) * (i - 1)));
                    } else {
                        low_double = (((diff_double / totalnodes) * (i - 1)) + 1);
                    }
                    up_double = ((diff_double / totalnodes) * (i));
                    if (reverseloop) {
                        lower = "" + (min_double - low_double);

                        if (i == totalnodes) {
                            upper = "" + ((max_double - (min_double - up_double)) + (min_double - up_double));
                        } else {

                            upper = "" + (min_double - up_double);
                        }
                    } else {
                        lower = "" + (min_double + low_double);
                        if (i == totalnodes) {
                            upper = "" + ((max_double - (min_double + up_double)) + (min_double + up_double));
                        } else {

                            upper = "" + (min_double + up_double);
                        }

                    }
                    result.add(new ParallelForSENP(i - 1, lower, upper, get.getUuid(), chunksize));
                    break;

            }

        }
        this.nodes = nodes.size();
        this.selectedNodes = nodes.size();
        backupNodes.addAll(nodes);
        totalChunks = result.size();
        return result;
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
        return totalChunks;
    }

    @Override
    public int getSelectedNodes() {
        return selectedNodes;
    }

}
