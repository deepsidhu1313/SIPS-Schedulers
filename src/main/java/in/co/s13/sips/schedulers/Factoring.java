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

/**
 *
 * Factoring Scheduler
 *
 * @author nika
 */
public class Factoring implements Scheduler {

    int nodes;

    @Override
    public ArrayList<TaskNodePair> schedule() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> livenodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodesList = new ArrayList<>();
        nodesList.addAll(livenodes.values());

        System.out.println("Before Sorting:" + nodesList);
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodesList, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
        System.out.println("After Sorting:" + nodesList);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);
        if (maxNodes < nodesList.size()) {
            // select best nodes for scheduling
            nodesList = new ArrayList<>(nodesList.subList(0, maxNodes));
        }
        String chunksize, lower, upper;
        boolean reverseloop = loop.isReverse();
        byte min_byte = 0, max_byte = 0, diff_byte = 0, low_byte, up_byte, cs_byte, lupper_byte = 0;
        short min_short = 0, max_short = 0, diff_short = 0, low_short, up_short, cs_short, lupper_short = 0;
        int min_int = 0, max_int = 0, diff_int = 0, low_int, up_int, cs_int, lupper_int = 0;
        long min_long = 0, max_long = 0, diff_long = 0, low_long, up_long, cs_long, lupper_long = 0;
        float min_float = 0, max_float = 0, diff_float = 0, low_float, up_float, cs_float, lupper_float = 0;
        double min_double = 0, max_double = 0, diff_double = 0, low_double, up_double, cs_double, lupper_double = 0;

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
        int totalnodes = nodesList.size();
        boolean chunksCreated = false;
        int i = 1;
        while ((!chunksCreated)) {

            switch (loop.getDataType()) {
                case 0:
                    cs_byte = (byte) Math.ceil((double) diff_byte / (double) (2 * totalnodes));
                    if (cs_byte < 1) {
                        cs_byte = 1;
                    }
                    chunksize = "" + cs_byte;

                    if (reverseloop) {
                        if (i == 1) {
                            low_byte = (byte) (0);
                            low_byte = (byte) (min_byte - low_byte);

                        } else {
                            low_byte = (byte) (lupper_byte - 1);
                        }

                        lower = "" + (low_byte);
                        up_byte = (byte) (low_byte - cs_byte );
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

                        up_byte = (byte) (low_byte + cs_byte);
                        upper = "" + (up_byte);
                        if (up_byte >= max_byte) {
                            upper = "" + (max_byte);
                            chunksCreated = true;
                        }
                        lupper_byte = up_byte;

                    }
                    if (i % totalnodes == 0) {
                        diff_byte = (byte) (diff_byte - (cs_byte * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
                case 1:
                    cs_short = (short) Math.ceil((double) diff_short / (double) (2 * totalnodes));
                    if (cs_short < 1) {
                        cs_short = 1;
                    }
                    chunksize = "" + cs_short;

                    if (reverseloop) {
                        if (i == 1) {
                            low_short = (short) (0);
                            low_short = (short) (min_short - low_short);

                        } else {
                            low_short = (short) (lupper_short - 1);
                        }

                        lower = "" + (low_short);
                        up_short = (short) (low_short - cs_short );
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

                        up_short = (short) (low_short + cs_short );
                        upper = "" + (up_short);
                        if (up_short >= max_short) {
                            upper = "" + (max_short);
                            chunksCreated = true;
                        }
                        lupper_short = up_short;

                    }
                    if (i % totalnodes == 0) {
                        diff_short = (short) (diff_short - (cs_short * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
                case 2:
                    cs_int = (int) Math.ceil((double) diff_int / (double) (2 * totalnodes));
                    if (cs_int < 1) {
                        cs_int = 1;
                    }
                    chunksize = "" + cs_int;

                    if (reverseloop) {
                        if (i == 1) {
                            low_int = (int) (0);
                            low_int = (int) (min_int - low_int);

                        } else {
                            low_int = (int) (lupper_int - 1);
                        }

                        lower = "" + (low_int);
                        up_int = (int) (low_int - cs_int );
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

                        up_int = (int) (low_int + cs_int );
                        upper = "" + (up_int);
                        if (up_int >= max_int) {
                            upper = "" + (max_int);
                            chunksCreated = true;
                        }
                        lupper_int = up_int;

                    }
                    if (i % totalnodes == 0) {
                        diff_int = (int) (diff_int - (cs_int * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
                case 3:
                    cs_long = (long) Math.ceil((double) diff_long / ((double) 2 * totalnodes));
                    if (cs_long < 1) {
                        cs_long = 1;
                    }
                    chunksize = "" + cs_long;

                    if (reverseloop) {
                        if (i == 1) {
                            low_long = (long) (0);
                            low_long = (long) (min_long - low_long);

                        } else {
                            low_long = (long) (lupper_long - 1);
                        }

                        lower = "" + (low_long);
                        up_long = (long) (low_long - cs_long );
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

                        up_long = (long) (low_long + cs_long );
                        upper = "" + (up_long);
                        if (up_long >= max_long) {
                            upper = "" + (max_long);
                            chunksCreated = true;
                        }
                        lupper_long = up_long;

                    }
                    if (i % totalnodes == 0) {
                        diff_long = (long) (diff_long - (cs_long * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
                case 4:
                    cs_float = (float) Math.ceil((double) diff_float / (double) (2 * totalnodes));
                    if (cs_float < 1) {
                        cs_float = 1;
                    }
                    chunksize = "" + cs_float;

                    if (reverseloop) {
                        if (i == 1) {
                            low_float = (float) (0);
                            low_float = (float) (min_float - low_float);

                        } else {
                            low_float = (float) (lupper_float - 1);
                        }

                        lower = "" + (low_float);
                        up_float = (float) (low_float - cs_float );
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

                        up_float = (float) (low_float + cs_float );
                        upper = "" + (up_float);
                        if (up_float >= max_float) {
                            upper = "" + (max_float);
                            chunksCreated = true;
                        }
                        lupper_float = up_float;

                    }
                    if (i % totalnodes == 0) {
                        diff_float = (float) (diff_float - (cs_float * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
                case 5:
                    cs_double = (double) Math.ceil((double) diff_double / (double) (2 * totalnodes));
                    if (cs_double < 1) {
                        cs_double = 1;
                    }
                    chunksize = "" + cs_double;

                    if (reverseloop) {
                        if (i == 1) {
                            low_double = (double) (0);
                            low_double = (double) (min_double - low_double);

                        } else {
                            low_double = (double) (lupper_double - 1);
                        }

                        lower = "" + (low_double);
                        up_double = (double) (low_double - cs_double );
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

                        up_double = (double) (low_double + cs_double );
                        upper = "" + (up_double);
                        if (up_double >= max_double) {
                            upper = "" + (max_double);
                            chunksCreated = true;
                        }
                        lupper_double = up_double;

                    }
                    if (i % totalnodes == 0) {
                        diff_double = (double) (diff_double - (cs_double * totalnodes));
                    }
                    result.add(new ParallelForSENP(lower, upper,"",chunksize));
                    break;
            }
            i++;
        }
        i = 0;
        for (int j = 0; j < result.size(); j++) {
            if (i == nodesList.size() - 1) {
                i = 0;
            }
            ParallelForSENP get = result.get(j);
            get.setNodeUUID(nodesList.get(i).getUuid());
            i++;
        }
        this.nodes = nodesList.size();
        return result;
    }

    @Override
    public int getTotalNodes() {
        return this.nodes;
    }
}
