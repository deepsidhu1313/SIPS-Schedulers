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

import java.util.ArrayList;
import java.util.Comparator;

/**
 *
 * @author nika
 */
public class Processor {

    private String id;
    private int availSlots;
    private double CPUScore;
    private double performanceFactor;
    private ArrayList<Task> que= new ArrayList<>();
    private ArrayList<String> depque= new ArrayList<>();
    private Double timeCounter = 0.0;
    private Double distanceFromCurrent = 0.0;

    public Processor(String id, int availSlots, double cpuScore, ArrayList<Task> que, ArrayList<String> depque, Double distanceFromCurrent, double performanceFactor) {
        this.id = id;
        this.availSlots = availSlots;
        this.CPUScore = cpuScore;
        this.que = que;
        this.depque = depque;
        this.distanceFromCurrent = distanceFromCurrent;
        this.performanceFactor = performanceFactor;
    }

    public Processor(Processor otherProcessor) {
        this.id = otherProcessor.id;
        this.availSlots = otherProcessor.availSlots;
        this.CPUScore = otherProcessor.CPUScore;
        for (int i = 0; i < otherProcessor.que.size(); i++) {
            Task get = otherProcessor.que.get(i);
            this.que.add(get);
        }
        for (int i = 0; i < otherProcessor.depque.size(); i++) {
            String get = otherProcessor.depque.get(i);
            this.depque.add(get);
        }
        this.distanceFromCurrent = otherProcessor.distanceFromCurrent;
        this.performanceFactor = otherProcessor.performanceFactor;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getAvailSlots() {
        return availSlots;
    }

    public void setAvailSlots(int availSlots) {
        this.availSlots = availSlots;
    }

    public ArrayList<Task> getQue() {
        return que;
    }

    public void setQue(ArrayList<Task> value) {
        this.que = value;
    }

    public ArrayList<String> getDepque() {
        return depque;
    }

    public void setDepque(ArrayList<String> value) {
        this.depque = value;
    }

    public double getCPUScore() {
        return CPUScore;
    }

    public void setCPUScore(double CPUScore) {
        this.CPUScore = CPUScore;
    }

    public Double getTimeCounter() {
        return timeCounter;
    }

    public Double incrementTimeCounter(Double delta) {
        return timeCounter += delta;
    }

    public void setTimeCounter(Double timeCounter) {
        this.timeCounter = timeCounter;
    }

    public Double getDistanceFromCurrent() {
        return distanceFromCurrent;
    }

    public void setDistanceFromCurrent(Double distanceFromCurrent) {
        this.distanceFromCurrent = distanceFromCurrent;
    }

    public double getPerformanceFactor() {
        return performanceFactor;
    }

    public void setPerformanceFactor(double performanceFactor) {
        this.performanceFactor = performanceFactor;
    }

    @Override
    public String toString() {
        return "Processor{" + "id=" + id + ", availSlots=" + availSlots + ", CPUScore=" + CPUScore + ", performanceFactor=" + performanceFactor + ", que=" + que + ", depque=" + depque + ", timeCounter=" + timeCounter + ", distanceFromCurrent=" + distanceFromCurrent + '}';
    }

    public ArrayList<FreeSlot> getFreeSlots() {
        ArrayList<FreeSlot> result = new ArrayList<>();
        ArrayList<Task> queue = this.getQue();
        if (queue.size() > 1) {
            for (int i = 0; i < queue.size() - 1; i++) {
                Task get = queue.get(i);
                Task get1 = queue.get(i + 1);
                Double diff = get1.getStarttime() - get.getEndtime();
                if (diff > 0) {
                    result.add(new FreeSlot(get.getEndtime(), get1.getStarttime(), diff));
                }
            }
        } else if (queue.size() == 1) {
            Task get = queue.get(0);
            Double freeSlot = get.getStarttime() - 0;
            if (freeSlot > 0) {
                result.add(new FreeSlot(0.0, get.getStarttime(), freeSlot));
            }
        } else if (queue.isEmpty()) {
            result.add(new FreeSlot(0.0, Double.MAX_VALUE, Double.MAX_VALUE));
        }
        return result;
    }

    public static enum ProcessorComparator implements Comparator<Processor> {

        CPU_SCORE_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Double.valueOf(o1.getCPUScore()).compareTo(o2.getCPUScore());
            }
        }, DISTANCE_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Double.valueOf(o1.getDistanceFromCurrent()).compareTo(o2.getDistanceFromCurrent());
            }
        }, AVAIL_SLOTS_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Integer.valueOf(o1.getAvailSlots()).compareTo(o2.getAvailSlots());
            }
        }, TIME_COUNTER_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Double.valueOf(o1.getTimeCounter()).compareTo(o2.getTimeCounter());
            }
        }, QUEUE_SIZE_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Integer.valueOf(o1.getQue().size()).compareTo(o2.getQue().size());
            }
        }, DEP_QUEUE_SIZE_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Integer.valueOf(o1.getDepque().size()).compareTo(o2.getDepque().size());
            }
        }, PF_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Double.valueOf(o1.getPerformanceFactor()).compareTo(o2.getPerformanceFactor());
            }
        }, FREE_SLOTS_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Integer.valueOf(o1.getFreeSlots().size()).compareTo(o2.getFreeSlots().size());
            }
        };

        public static Comparator<Processor> decending(final Comparator<Processor> other) {
            return (Processor o1, Processor o2) -> -1 * other.compare(o1, o2);
        }

        public static Comparator<Processor> getComparator(final ProcessorComparator... multipleOptions) {
            return (Processor o1, Processor o2) -> {
                for (ProcessorComparator option : multipleOptions) {
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
