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

public class QSS implements Scheduler {

    private int nodes, totalChunks, selectedNodes;
    private ArrayList<Node> backupNodes = new ArrayList<>();

    @Override
    public int getSelectedNodes() {
        return selectedNodes;
    }

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
        double QSSLCFactor = schedulerSettings.getDouble("LCFactor", 0.001);
        double QSSdelta = schedulerSettings.getDouble("delta", 2.0);
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
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte, fc_byte, lc_byte, cs_byte, lcs_byte, QSSn_byte, QSSa_byte = 0, QSSb_byte = 0, QSSc_byte = 0, lupper_byte = 0;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short, fc_short, lc_short, cs_short, lcs_short, QSSn_short, QSSa_short = 0, QSSb_short = 0, QSSc_short = 0, lupper_short = 0;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int, fc_int, lc_int, cs_int, lcs_int, QSSn_int, QSSa_int = 0, QSSb_int = 0, QSSc_int = 0, lupper_int = 0;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long, fc_long, lc_long, cs_long, lcs_long, QSSn_long, QSSa_long = 0, QSSb_long = 0, QSSc_long = 0, lupper_long = 0;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float, fc_float, lc_float, cs_float, lcs_float, QSSn_float, QSSa_float = 0, QSSb_float = 0, QSSc_float = 0, lupper_float = 0;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double, fc_double, lc_double, cs_double, lcs_double, QSSn_double, QSSa_double = 0, QSSb_double = 0, QSSc_double = 0, lupper_double = 0;
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
                        fc_byte = (byte) (diff_byte / (2 * totalnodes));
                        lc_byte = (byte) (QSSLCFactor * diff_byte);
                        if (lc_byte < 1) {
                            lc_byte = 1;
                        }
                        byte CN2 = (byte) ((fc_byte + lc_byte) / (QSSdelta));
                        QSSn_byte = (byte) ((6 * diff_byte) / ((4 * CN2) + lc_byte + fc_byte));
                        QSSa_byte = fc_byte;
                        QSSb_byte = (byte) (((4 * CN2) - lc_byte - (3 * fc_byte)) / QSSn_byte);
                        QSSc_byte = (byte) (((2 * fc_byte) + (2 * lc_byte) - (4 * CN2)) / (QSSn_byte * QSSn_byte));
                        cs_byte = fc_byte;
                    } else {
                        cs_byte = (byte) (QSSa_byte + (QSSb_byte * i) + (QSSc_byte * (i * i)));
                    }
                    if (cs_byte < 1) {
                        cs_byte = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;
                case 1:
                    if (i == 1) {
                        fc_short = (short) (diff_short / (2 * totalnodes));
                        lc_short = (short) (QSSLCFactor * diff_short);
                        if (lc_short < 1) {
                            lc_short = 1;
                        }
                        short CN2 = (short) ((fc_short + lc_short) / (QSSdelta));
                        QSSn_short = (short) ((6 * diff_short) / ((4 * CN2) + lc_short + fc_short));
                        QSSa_short = fc_short;
                        QSSb_short = (short) (((4 * CN2) - lc_short - (3 * fc_short)) / QSSn_short);
                        QSSc_short = (short) (((2 * fc_short) + (2 * lc_short) - (4 * CN2)) / (QSSn_short * QSSn_short));
                        cs_short = fc_short;
                    } else {
                        cs_short = (short) (QSSa_short + (QSSb_short * i) + (QSSc_short * (i * i)));
                    }

                    if (cs_short < 1) {
                        cs_short = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;
                case 2:
                    if (i == 1) {
                        fc_int = (int) (diff_int / (2 * totalnodes));
                        lc_int = (int) (QSSLCFactor * diff_int);
                        if (lc_int < 1) {
                            lc_int = 1;
                        }

                        int CN2 = (int) ((fc_int + lc_int) / (QSSdelta));
                        QSSn_int = (int) ((6 * diff_int) / ((4 * CN2) + lc_int + fc_int));
                        QSSa_int = fc_int;
                        QSSb_int = (int) (((4 * CN2) - lc_int - (3 * fc_int)) / QSSn_int);
                        QSSc_int = (int) (((2 * fc_int) + (2 * lc_int) - (4 * CN2)) / (QSSn_int * QSSn_int));
                        cs_int = fc_int;
                    } else {
                        cs_int = (int) (QSSa_int + (QSSb_int * i) + (QSSc_int * (i * i)));
                    }

                    if (cs_int < 1) {
                        cs_int = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;
                case 3:

                    if (i == 1) {
                        fc_long = (long) (diff_long / (2 * totalnodes));
                        lc_long = (long) (QSSLCFactor * diff_long);
                        if (lc_long < 1) {
                            lc_long = 1;
                        }

                        long CN2 = (long) ((fc_long + lc_long) / (QSSdelta));
                        QSSn_long = (long) ((6 * diff_long) / ((4 * CN2) + lc_long + fc_long));
                        QSSa_long = fc_long;
                        QSSb_long = (long) (((4 * CN2) - lc_long - (3 * fc_long)) / QSSn_long);
                        QSSc_long = (long) (((2 * fc_long) + (2 * lc_long) - (4 * CN2)) / (QSSn_long * QSSn_long));
                        cs_long = fc_long;
                    } else {
                        cs_long = (long) (QSSa_long + (QSSb_long * i) + (QSSc_long * (i * i)));
                    }

                    if (cs_long < 1) {
                        cs_long = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;
                case 4:

                    if (i == 1) {
                        fc_float = (float) (diff_float / (2 * totalnodes));
                        lc_float = (float) (QSSLCFactor * diff_float);
                        if (lc_float < 1) {
                            lc_float = 1;
                        }
                        float CN2 = (float) ((fc_float + lc_float) / (QSSdelta));
                        QSSn_float = (float) ((6 * diff_float) / ((4 * CN2) + lc_float + fc_float));
                        QSSa_float = fc_float;
                        QSSb_float = (float) (((4 * CN2) - lc_float - (3 * fc_float)) / QSSn_float);
                        QSSc_float = (float) (((2 * fc_float) + (2 * lc_float) - (4 * CN2)) / (QSSn_float * QSSn_float));
                        cs_float = fc_float;
                    } else {
                        cs_float = (float) (QSSa_float + (QSSb_float * i) + (QSSc_float * (i * i)));
                    }

                    if (cs_float < 1) {
                        cs_float = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;
                case 5:

                    if (i == 1) {
                        fc_double = (double) (diff_double / (2 * totalnodes));
                        lc_double = (double) (QSSLCFactor * diff_double);
                        if (lc_double < 1) {
                            lc_double = 1;
                        }

                        double CN2 = (double) ((fc_double + lc_double) / (QSSdelta));
                        QSSn_double = (double) ((6 * diff_double) / ((4 * CN2) + lc_double + fc_double));
                        QSSa_double = fc_double;
                        QSSb_double = (double) (((4 * CN2) - lc_double - (3 * fc_double)) / QSSn_double);
                        QSSc_double = (double) (((2 * fc_double) + (2 * lc_double) - (4 * CN2)) / (QSSn_double * QSSn_double));
                        cs_double = fc_double;
                    } else {
                        cs_double = (double) (QSSa_double + (QSSb_double * i) + (QSSc_double * (i * i)));
                    }

                    if (cs_double < 1) {
                        cs_double = 1;
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
                    result.add(new ParallelForSENP(i - 1, lower, upper, "", chunksize));
                    break;

            }
            i++;
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
}
