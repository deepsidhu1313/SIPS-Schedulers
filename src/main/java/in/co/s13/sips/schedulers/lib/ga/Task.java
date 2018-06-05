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
package in.co.s13.sips.schedulers.lib.ga;

import in.co.s13.sips.lib.common.datastructure.ParallelForSENP;
import in.co.s13.sips.lib.common.datastructure.SIPSTask;
import java.util.ArrayList;
import java.util.Comparator;

/**
 *
 * @author nika
 */
public class Task {

    private String id;
    private double value, problemSize;
    private long starttime;
    private long endtime;
    private long exectime;
    private ArrayList<Task> deplist = new ArrayList<>();
    private ParallelForSENP parallelForLoop;
    private SIPSTask sipsTask;

    public Task(String id, double value, long starttime, long endtime, long exectime, ArrayList<Task> deplist, double problemSize) {
        this.id = id;
        this.value = value;
        this.starttime = starttime;
        this.endtime = endtime;
        this.deplist = deplist;
        this.exectime = exectime;
        this.problemSize = problemSize;
    }

    public Task(Task otherTask) {
        this.id = otherTask.id;
        this.value = otherTask.value;
        this.starttime = otherTask.starttime;
        this.endtime = otherTask.endtime;
        this.deplist = otherTask.deplist;
        this.exectime = otherTask.exectime;
        this.problemSize = otherTask.problemSize;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getStarttime() {
        return starttime;
    }

    public void setStarttime(long value) {
        this.starttime = value;
    }

    public long getEndtime() {
        return endtime;
    }

    public void setEndtime(long value) {
        this.endtime = value;
    }

    public long getExectime() {
        return exectime;
    }

    public void setExectime(long value) {
        this.exectime = value;
    }

    public ArrayList<Task> getDeplist() {
        return deplist;
    }

    public void setDeplist(ArrayList<Task> value) {
        this.deplist = value;
    }

    public ParallelForSENP getParallelForLoop() {
        return parallelForLoop;
    }

    public void setParallelForLoop(ParallelForSENP parallelForLoop) {
        this.parallelForLoop = parallelForLoop;
    }

    public SIPSTask getSipsTask() {
        return sipsTask;
    }

    public void setSipsTask(SIPSTask sipsTask) {
        this.sipsTask = sipsTask;
    }

    public double getProblemSize() {
        return problemSize;
    }

    public void setProblemSize(double problemSize) {
        this.problemSize = problemSize;
    }

    public String getNodeUUID() {
        if (this.parallelForLoop != null) {
            return this.parallelForLoop.getNodeUUID();
        } else if (this.sipsTask != null) {
            return this.sipsTask.getNodeUUID();
        }
        return "Empty";
    }

    @Override
    public String toString() {
        return "Task{" + "id=" + id + ", value=" + value + ", problemSize=" + problemSize + ", starttime=" + starttime + ", endtime=" + endtime + ", exectime=" + exectime + ", deplist=" + deplist + '}';
    }

   

    public static enum TaskComparator implements Comparator<Task> {

        VALUE_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Double.valueOf(o1.getValue()).compareTo(o2.getValue());
            }
        }, START_TIME_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Long.valueOf(o1.getStarttime()).compareTo(o2.getStarttime());
            }
        }, END_TIME_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Long.valueOf(o1.getEndtime()).compareTo(o2.getEndtime());
            }
        }, EXC_TIME_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Long.valueOf(o1.getExectime()).compareTo(o2.getExectime());
            }
        },
        DEP_LIST_SIZE_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Integer.valueOf(o1.getDeplist().size()).compareTo(o2.getDeplist().size());
            }
        };

        public static Comparator<Task> decending(final Comparator<Task> other) {
            return (Task o1, Task o2) -> -1 * other.compare(o1, o2);
        }

        public static Comparator<Task> getComparator(final TaskComparator... multipleOptions) {
            return (Task o1, Task o2) -> {
                for (TaskComparator option : multipleOptions) {
                    int result = option.compare(o1, o2);
                    if (result != 0) {
                        return result;
                    }
                }
                return 0;
            };
        }

    }
}
