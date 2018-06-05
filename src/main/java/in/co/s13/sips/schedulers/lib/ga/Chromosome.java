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
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author nika
 */
public class Chromosome {

    private ArrayList<Task> elements = new ArrayList<>();
    private ArrayList<Processor> processors = new ArrayList<>();
    private ConcurrentHashMap<String, Task> elementsHM = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Processor> processorsHM = new ConcurrentHashMap<>();
    private long scheduleLength = 0;

    public Chromosome(ArrayList<Task> elements, ArrayList<Processor> processors) {
        this.elements = elements;
        this.processors = processors;
    }

    public Chromosome() {
    }

    public ArrayList<Task> getElements() {
        return elements;
    }

    public void setElements(ArrayList<Task> elements) {
        this.elements = elements;
    }

    public ArrayList<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(ArrayList<Processor> processors) {
        this.processors = processors;
    }

    public long getScheduleLength() {
        return scheduleLength;
    }

    public void setScheduleLength(long scheduleLength) {
        this.scheduleLength = scheduleLength;
    }

    public void addElementsToHashMap() {
        elementsHM.clear();
        for (int i = 0; i < elements.size(); i++) {
            Task get = elements.get(i);
            elementsHM.put(get.getId(), get);
        }
        processorsHM.clear();
        for (int i = 0; i < processors.size(); i++) {
            Processor get = processors.get(i);
            processorsHM.put(get.getId(), get);
        }
    }

    
    
    public ConcurrentHashMap<String, Task> getElementsHM() {
        return elementsHM;
    }

    public void setElementsHM(ConcurrentHashMap<String, Task> elementsHM) {
        this.elementsHM = elementsHM;
    }

    public ConcurrentHashMap<String, Processor> getProcessorsHM() {
        return processorsHM;
    }

    public void setProcessorsHM(ConcurrentHashMap<String, Processor> processorsHM) {
        this.processorsHM = processorsHM;
    }

    @Override
    public String toString() {
        return "Chromosome{" + "elements=" + elements + ", processors=" + processors + ", elementsHM=" + elementsHM + ", processorsHM=" + processorsHM + ", scheduleLength=" + scheduleLength + '}';
    }
    
     public static enum ChromosomeComparator implements Comparator<Chromosome> {

        SCHEDULE_LENGTH_SORT {
            @Override
            public int compare(Chromosome o1, Chromosome o2) {
                return Long.valueOf(o1.getScheduleLength()).compareTo(o2.getScheduleLength());
            }
        };

        public static Comparator<Chromosome> decending(final Comparator<Chromosome> other) {
            return (Chromosome o1, Chromosome o2) -> -1 * other.compare(o1, o2);
        }

        public static Comparator<Chromosome> getComparator(final ChromosomeComparator... multipleOptions) {
            return (Chromosome o1, Chromosome o2) -> {
                for (ChromosomeComparator option : multipleOptions) {
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
