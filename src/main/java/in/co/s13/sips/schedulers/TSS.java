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
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

public class TSS implements Scheduler {

    int nodes;

    @Override
    public ArrayList<TaskNodePair> schedule() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> livenodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(livenodes.values());

        System.out.println("Before Sorting:" + nodes);
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        System.out.println("After Sorting:" + nodes);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);
        double FCFactor = schedulerSettings.getDouble("FCFactor", 0.07);
        double LCFactor = schedulerSettings.getDouble("LCFactor", 0.001);
        if (maxNodes > 8) {
            Node node = livenodes.get(in.co.s13.sips.lib.node.settings.GlobalValues.NODE_UUID);
            nodes.remove(node);
        }
        if (maxNodes < nodes.size()) {
            // select best nodes for scheduling
            nodes = new ArrayList<>(nodes.subList(0, maxNodes));
        }
        String chunksize, lower, upper;
        boolean reverseloop = loop.isReverse();
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte, cs_byte, lupper_byte = 0, fc_byte, lc_byte, TSSred_byte = 0, lcs_byte = 0;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short, cs_short, lupper_short = 0, fc_short, lc_short, TSSred_short = 0, lcs_short = 0;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int, cs_int, lupper_int = 0, fc_int, lc_int, TSSred_int = 0, lcs_int = 0;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long, cs_long, lupper_long = 0, fc_long, lc_long, TSSred_long = 0, lcs_long = 0;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float, cs_float, lupper_float = 0, fc_float, lc_float, TSSred_float = 0, lcs_float = 0;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double, cs_double, lupper_double = 0, fc_double, lc_double, TSSred_double = 0, lcs_double = 0;

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

        boolean chunksCreated = false;
        int i = 1;
        while ((!chunksCreated)) {

            switch (loop.getDataType()) {
                case 0:
                    if (i == 1) {
                        fc_byte = (byte) (FCFactor * diff_byte);
                        lc_byte = (byte) (LCFactor * diff_byte);
                        byte M = (byte) ((2 * diff_byte) / (fc_byte + lc_byte));
                        TSSred_byte = (byte) ((fc_byte - lc_byte) / (M - 1));
                        cs_byte = fc_byte;
                    } else {
                        cs_byte = (byte) (lcs_byte - TSSred_byte);
                    }
                    lcs_byte = cs_byte;

                    chunksize = "" + cs_byte;

                    if (reverseloop) {
                        if (i == 1) {
                            low_byte = (byte) (0);
                            low_byte = (byte) (min_byte - low_byte);

                        } else {
                            low_byte = (byte) (lupper_byte - 1);
                        }

                        lower = "" + (low_byte);
                        up_byte = (byte) (low_byte - cs_byte + 1);
                        upper = "" + (up_byte);

                        if (lupper_byte <= max_byte) {
                            upper = "" + (max_byte);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_byte = (byte) (0);
                            low_byte = (byte) (min_byte + low_byte);

                        } else {

                            low_byte = (byte) (lupper_byte + 1);

                        }
                        lower = "" + (low_byte);

                        up_byte = (byte) (low_byte + cs_byte - 1);
                        upper = "" + (up_byte);
                        if (up_byte >= max_byte) {
                            upper = "" + (max_byte);
                            chunksCreated = true;
                        }
                        lupper_byte = up_byte;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
                case 1:
                    if (i == 1) {
                        fc_short = (short) (FCFactor * diff_short);
                        lc_short = (short) (LCFactor * diff_short);
                        short M = (short) ((2 * diff_short) / (fc_short + lc_short));
                        TSSred_short = (short) ((fc_short - lc_short) / (M - 1));
                        cs_short = fc_short;
                    } else {
                        cs_short = (short) (lcs_short - TSSred_short);
                    }
                    lcs_short = cs_short;
                    chunksize = "" + cs_short;

                    if (reverseloop) {
                        if (i == 1) {
                            low_short = (short) (0);
                            low_short = (short) (min_short - low_short);

                        } else {
                            low_short = (short) (lupper_short - 1);
                        }

                        lower = "" + (low_short);
                        up_short = (short) (low_short - cs_short + 1);
                        upper = "" + (up_short);

                        if (lupper_short <= max_short) {
                            upper = "" + (max_short);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_short = (short) (0);
                            low_short = (short) (min_short + low_short);

                        } else {

                            low_short = (short) (lupper_short + 1);

                        }
                        lower = "" + (low_short);

                        up_short = (short) (low_short + cs_short - 1);
                        upper = "" + (up_short);
                        if (up_short >= max_short) {
                            upper = "" + (max_short);
                            chunksCreated = true;
                        }
                        lupper_short = up_short;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
                case 2:

                    if (i == 1) {
                        fc_int = (int) (FCFactor * diff_int);
                        lc_int = (int) (LCFactor * diff_int);
                        int M = (int) ((2 * diff_int) / (fc_int + lc_int));
                        TSSred_int = (int) ((fc_int - lc_int) / (M - 1));
                        cs_int = fc_int;
                    } else {
                        cs_int = (int) (lcs_int - TSSred_int);
                    }
                    lcs_int = cs_int;
                    chunksize = "" + cs_int;

                    if (reverseloop) {
                        if (i == 1) {
                            low_int = (int) (0);
                            low_int = (int) (min_int - low_int);

                        } else {
                            low_int = (int) (lupper_int - 1);
                        }

                        lower = "" + (low_int);
                        up_int = (int) (low_int - cs_int + 1);
                        upper = "" + (up_int);

                        if (lupper_int <= max_int) {
                            upper = "" + (max_int);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_int = (int) (0);
                            low_int = (int) (min_int + low_int);

                        } else {

                            low_int = (int) (lupper_int + 1);

                        }
                        lower = "" + (low_int);

                        up_int = (int) (low_int + cs_int - 1);
                        upper = "" + (up_int);
                        if (up_int >= max_int) {
                            upper = "" + (max_int);
                            chunksCreated = true;
                        }
                        lupper_int = up_int;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
                case 3:

                    if (i == 1) {
                        fc_long = (long) (FCFactor * diff_long);
                        lc_long = (long) (LCFactor * diff_long);
                        long M = (long) ((2 * diff_long) / (fc_long + lc_long));
                        TSSred_long = (long) ((fc_long - lc_long) / (M - 1));
                        cs_long = fc_long;
                    } else {
                        cs_long = (long) (lcs_long - TSSred_long);
                    }
                    lcs_long = cs_long;
                    chunksize = "" + cs_long;

                    if (reverseloop) {
                        if (i == 1) {
                            low_long = (long) (0);
                            low_long = (long) (min_long - low_long);

                        } else {
                            low_long = (long) (lupper_long - 1);
                        }

                        lower = "" + (low_long);
                        up_long = (long) (low_long - cs_long + 1);
                        upper = "" + (up_long);

                        if (lupper_long <= max_long) {
                            upper = "" + (max_long);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_long = (long) (0);
                            low_long = (long) (min_long + low_long);

                        } else {

                            low_long = (long) (lupper_long + 1);

                        }
                        lower = "" + (low_long);

                        up_long = (long) (low_long + cs_long - 1);
                        upper = "" + (up_long);
                        if (up_long >= max_long) {
                            upper = "" + (max_long);
                            chunksCreated = true;
                        }
                        lupper_long = up_long;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
                case 4:

                    if (i == 1) {
                        fc_float = (float) (FCFactor * diff_float);
                        lc_float = (float) (LCFactor * diff_float);
                        float M = (float) ((2 * diff_float) / (fc_float + lc_float));
                        TSSred_float = (float) ((fc_float - lc_float) / (M - 1));
                        cs_float = fc_float;
                    } else {
                        cs_float = (float) (lcs_float - TSSred_float);
                    }
                    lcs_float = cs_float;
                    chunksize = "" + cs_float;

                    if (reverseloop) {
                        if (i == 1) {
                            low_float = (float) (0);
                            low_float = (float) (min_float - low_float);

                        } else {
                            low_float = (float) (lupper_float - 1);
                        }

                        lower = "" + (low_float);
                        up_float = (float) (low_float - cs_float + 1);
                        upper = "" + (up_float);

                        if (lupper_float <= max_float) {
                            upper = "" + (max_float);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_float = (float) (0);
                            low_float = (float) (min_float + low_float);

                        } else {

                            low_float = (float) (lupper_float + 1);

                        }
                        lower = "" + (low_float);

                        up_float = (float) (low_float + cs_float - 1);
                        upper = "" + (up_float);
                        if (up_float >= max_float) {
                            upper = "" + (max_float);
                            chunksCreated = true;
                        }
                        lupper_float = up_float;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
                case 5:

                    if (i == 1) {
                        fc_double = (double) (FCFactor * diff_double);
                        lc_double = (double) (LCFactor * diff_double);
                        double M = (double) ((2 * diff_double) / (fc_double + lc_double));
                        TSSred_double = (double) ((fc_double - lc_double) / (M - 1));
                        cs_double = fc_double;
                    } else {
                        cs_double = (double) (lcs_double - TSSred_double);
                    }
                    lcs_double = cs_double;
                    chunksize = "" + cs_double;

                    if (reverseloop) {
                        if (i == 1) {
                            low_double = (double) (0);
                            low_double = (double) (min_double - low_double);

                        } else {
                            low_double = (double) (lupper_double - 1);
                        }

                        lower = "" + (low_double);
                        up_double = (double) (low_double - cs_double + 1);
                        upper = "" + (up_double);

                        if (lupper_double <= max_double) {
                            upper = "" + (max_double);
                            chunksCreated = true;
                        }

                    } else {

                        if (i == 1) {

                            low_double = (double) (0);
                            low_double = (double) (min_double + low_double);

                        } else {

                            low_double = (double) (lupper_double + 1);

                        }
                        lower = "" + (low_double);

                        up_double = (double) (low_double + cs_double - 1);
                        upper = "" + (up_double);
                        if (up_double >= max_double) {
                            upper = "" + (max_double);
                            chunksCreated = true;
                        }
                        lupper_double = up_double;

                    }
                    result.add(new ParallelForSENP(lower, upper, "", chunksize));
                    break;
            }

        }
        i = 0;
        for (int j = 0; j < result.size(); j++) {
            if (i == nodes.size() - 1) {
                i = 0;
            }
            ParallelForSENP get = result.get(j);
            get.setNodeUUID(nodes.get(i).getUuid());
            i++;
        }
        this.nodes = nodes.size();
        return result;
    }

    @Override
    public int getTotalNodes() {
        return this.nodes;
    }
}
