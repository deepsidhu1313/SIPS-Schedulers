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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.json.JSONObject;

/**
 *
 * @author nika
 */
public class GA implements Scheduler {
    // Usually this can be a field rather than a method variable

    Random rand = new Random();
    private int nodes, totalChunks;
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
        ConcurrentHashMap<String, Processor> processors = new ConcurrentHashMap<>();
        processors.put(nodes.get(0).getUuid(), new Processor(nodes.get(0).getUuid(), availSlots, nodes.get(0).getCPUScore(), new ArrayList<Task>(), new ArrayList<>(), nodes.get(0).getDistanceFromCurrent(), maxCPUScore / nodes.get(0).getCPUScore()));
        
        for (int i = 1; i < nodes.size(); i++) {
            Node get = nodes.get(i);
            availSlots += (get.getTask_limit() - get.getWaiting_in_que());
            int availSlotsOnNode = (get.getTask_limit() - get.getWaiting_in_que());
            processors.put(get.getUuid(), new Processor(get.getUuid(), availSlots, get.getCPUScore(), new ArrayList<Task>(), new ArrayList<>(), get.getDistanceFromCurrent(), maxCPUScore / get.getCPUScore()));
            for (int j = 0; j < availSlotsOnNode; j++) {
                minExpectedTime = ((minExpectedTime) * (maxCPUScore / get.getCPUScore())) / ((minExpectedTime) + (maxCPUScore / get.getCPUScore()));
            }
        }
        if (availSlots < maxNodes) {
            availSlots = maxNodes;
        }
        System.out.println("Max Score: " + maxCPUScore + " Min Expected Time:" + minExpectedTime);
        
        byte diff_byte = 0, chunkFactor_byte;
        short diff_short = 0, chunkFactor_short;
        int diff_int = 0, chunkFactor_int;
        long diff_long = 0, chunkFactor_long;
        float diff_float = 0, chunkFactor_float;
        double diff_double = 0, chunkFactor_double;
        
        boolean chunksCreated = false;
        int i = 1;
        TSS tss = new TSS();
        result = tss.scheduleParallelFor(liveNodes, loop, schedulerSettings);
        i = 0;
        Chromosome resultant = new Chromosome();
        ArrayList<ProcessorComparator> comparators = new ArrayList<>();
        comparators.add(ProcessorComparator.AVAIL_SLOTS_SORT);
        comparators.add(ProcessorComparator.CPU_SCORE_SORT);
        comparators.add(ProcessorComparator.DISTANCE_SORT);
        comparators.add(ProcessorComparator.PF_SORT);
        ArrayList<Chromosome> chromosomes = new ArrayList<>();
        
        for (int r = 0; r < maxPopulation; r++) {
            chunkFactor_byte = 0;
            chunkFactor_short = 0;
            chunkFactor_int = 0;
            chunkFactor_long = 0;
            chunkFactor_float = 0;
            chunkFactor_double = 0;
            
            ArrayList<Task> elements = new ArrayList<>();
            ArrayList<Processor> processorsForSelection = new ArrayList<>();
            processors.values().forEach(value -> processorsForSelection.add(new Processor(value)));
            ProcessorComparator randomComparator = comparators.get(randInt(0, comparators.size()));
            Collections.sort(processorsForSelection, randomComparator);
            Chromosome chromosome = new Chromosome();
            chromosome.getProcessors().addAll(processorsForSelection);
            for (int j = 0; j < result.size(); j++) {
                Processor randomlySelectedNode = getRandomProcessor(processorsForSelection, processors);
                ParallelForSENP get = result.get(j);
                switch (loop.getDataType()) {
                    case 0:
                        chunkFactor_byte = Byte.parseByte(get.getDiff());
                        Task task_byte = new Task("" + j, (chunkFactor_byte * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_byte = new ParallelForSENP(get);
                        duplicate_byte.setNodeUUID(randomlySelectedNode.getId());
                        task_byte.setParallelForLoop(duplicate_byte);
                        randomlySelectedNode.getQue().add(task_byte);
                        elements.add(task_byte);
                        break;
                    case 1:
                        chunkFactor_short = Short.parseShort(get.getDiff());
                        Task task_short = new Task("" + j, (chunkFactor_short * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_short = new ParallelForSENP(get);
                        duplicate_short.setNodeUUID(randomlySelectedNode.getId());
                        task_short.setParallelForLoop(duplicate_short);
                        randomlySelectedNode.getQue().add(task_short);
                        elements.add(task_short);
                        break;
                    case 2:
                        chunkFactor_int = Integer.parseInt(get.getDiff());
                        Task task_int = new Task("" + j, (chunkFactor_int * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_int = new ParallelForSENP(get);
                        duplicate_int.setNodeUUID(randomlySelectedNode.getId());
                        task_int.setParallelForLoop(duplicate_int);
                        randomlySelectedNode.getQue().add(task_int);
                        elements.add(task_int);
                        break;
                    case 3:
                        chunkFactor_long = Long.parseLong(get.getDiff());
                        Task task_long = new Task("" + j, (chunkFactor_long * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_long = new ParallelForSENP(get);
                        duplicate_long.setNodeUUID(randomlySelectedNode.getId());
                        task_long.setParallelForLoop(duplicate_long);
                        randomlySelectedNode.getQue().add(task_long);
                        
                        elements.add(task_long);
                        break;
                    case 4:
                        chunkFactor_float = Float.parseFloat(get.getDiff());
                        Task task_float = new Task("" + j, (chunkFactor_float * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_float = new ParallelForSENP(get);
                        duplicate_float.setNodeUUID(randomlySelectedNode.getId());
                        task_float.setParallelForLoop(duplicate_float);
                        randomlySelectedNode.getQue().add(task_float);
                        elements.add(task_float);
                        break;
                    case 5:
                        chunkFactor_double = Double.parseDouble(get.getDiff());
                        Task task_double = new Task("" + j, (chunkFactor_double * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), new ArrayList<Task>());
                        ParallelForSENP duplicate_double = new ParallelForSENP(get);
                        duplicate_double.setNodeUUID(randomlySelectedNode.getId());
                        task_double.setParallelForLoop(duplicate_double);
                        randomlySelectedNode.getQue().add(task_double);
                        elements.add(task_double);
                        break;
                }
                
            }
            System.out.println("Elements:" + elements);
            chromosome.getElements().addAll(elements);
            chromosomes.add(chromosome);
        }
        
        Chromosome chromosome1 = chromosomes.get(randInt(0, chromosomes.size()));
        
        for (i = 0; i < maxGenerations; i++) {
            /**
             * *CrossOver**
             */
            boolean failedToChoosePoint = false;
            int counter = 0;
            Chromosome chromosome2 = chromosomes.get(randInt(0, chromosomes.size()));
            if (chromosome1.getElements().size() == chromosome2.getElements().size() && chromosome1.getElements().size() > 2) {
                int randomCrossOverPoint = randInt(0, chromosome1.getElements().size());
                while (randomCrossOverPoint >= (chromosome1.getElements().size() - 1)) {
                    randomCrossOverPoint = randInt(0, chromosome1.getElements().size());
                    if (counter > 10) {
                        failedToChoosePoint = true;
                    }
                    counter++;
                }
                if (failedToChoosePoint) {
                    randomCrossOverPoint = ((chromosome1.getElements().size() - 1) / 2);
                }
                System.out.println("Crossover at :" + randomCrossOverPoint + " on list of size: " + chromosome1.getElements().size());
                List<Task> first = chromosome1.getElements().subList(0, randomCrossOverPoint);
                System.out.println("Sublist 1:" + first);
                List<Task> second = chromosome1.getElements().subList(randomCrossOverPoint, chromosome1.getElements().size());
                System.out.println("Sublist 2:" + second);
                List<Task> third = chromosome2.getElements().subList(0, randomCrossOverPoint);
                System.out.println("Sublist 3:" + third);
                List<Task> fourth = chromosome2.getElements().subList(randomCrossOverPoint, chromosome2.getElements().size());
                System.out.println("Sublist 4:" + fourth);
                ArrayList<Task> temp1 = new ArrayList<>();
                temp1.addAll(first);
                temp1.addAll(fourth);
                ArrayList<Task> temp2 = new ArrayList<>();
                temp2.addAll(third);
                temp2.addAll(second);
                chromosome1.setElements(temp1);
                chromosome2.setElements(temp2);
                chromosome1.processors.clear();
                chromosome2.processors.clear();
                ArrayList<Processor> processorsForSelection = new ArrayList<>();
                ArrayList<Processor> processorsForSelection2 = new ArrayList<>();
                processors.values().forEach((value)
                        -> {
                    processorsForSelection.add(new Processor(value));
                    processorsForSelection2.add(new Processor(value));
                }
                );
                chromosome1.getProcessors().addAll(processorsForSelection);
                chromosome2.getProcessors().addAll(processorsForSelection2);
                chromosome1.addElementsToHashMap();
                chromosome2.addElementsToHashMap();
                reassignProcessorsAccToSlots(chromosome1);
                reassignProcessorsAccToSlots(chromosome2);
                
            }

            /**
             * Select best for mutation
             */
            Chromosome forMutation = bestForMutation(chromosome1, chromosome2);
            /**
             * *Mutation**
             */
            int randomPointForMutation = randInt(0, forMutation.getElements().size());
            Task task4Mutation = forMutation.getElements().get(randomPointForMutation);
            String idOfProcessorToBeReplaced = task4Mutation.getParallelForLoop().getNodeUUID();
            Processor toBeReplaced = forMutation.getProcessorsHM().get(idOfProcessorToBeReplaced);
            ArrayList<Processor> processorsList = forMutation.getProcessors();
            Collections.sort(processorsList, ProcessorComparator.CPU_SCORE_SORT.reversed());
            int randomIndexOfProcessor = randInt(0, processorsList.indexOf(toBeReplaced));
            task4Mutation.getParallelForLoop().setNodeUUID(processorsList.get(randomIndexOfProcessor).getId());
            resultant = forMutation;
        }
        reassignProcessorsAccToSlots(resultant);
        System.out.println("Best Chromosome:" + resultant);
        this.totalChunks = resultant.getElements().size();
        this.nodes = (int) resultant.getProcessors().stream().filter(p -> p.getQue().size() > 0).count();
        backupNodes.addAll(nodes);
        ArrayList<ParallelForSENP> result2 = new ArrayList<>();
        resultant.getElements().forEach(element -> result2.add(element.getParallelForLoop()));
        return result2;
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
        return this.totalChunks;
    }
    
    private Processor getRandomProcessor(ArrayList<Processor> processorsForSelection, ConcurrentHashMap<String, Processor> allNodes) {
        if (!processorsForSelection.isEmpty()) {
            int randomSelectedIndex = randInt(0, processorsForSelection.size());
            Processor random = processorsForSelection.get(randomSelectedIndex);
            int remainingSlotsOnProcessor = random.getAvailSlots() - random.getQue().size();
            if (remainingSlotsOnProcessor == 0) {
                processorsForSelection.remove(randomSelectedIndex);
            }
            return random;
        } else {
            ArrayList<Processor> otherOne = new ArrayList<>();
            otherOne.addAll(allNodes.values());
            return otherOne.get(randInt(0, otherOne.size()));
        }
    }
    
    private Chromosome bestForMutation(Chromosome chromosome1, Chromosome chromosome2) {
        ArrayList<Processor> duplicateList = new ArrayList<>();
        Collections.copy(duplicateList, chromosome1.getProcessors());
        Collections.sort(duplicateList, ProcessorComparator.TIME_COUNTER_SORT.reversed());
        ArrayList<Processor> duplicateList2 = new ArrayList<>();
        Collections.copy(duplicateList2, chromosome2.getProcessors());
        Collections.sort(duplicateList2, ProcessorComparator.TIME_COUNTER_SORT.reversed());
        if (duplicateList.size() < 1 || duplicateList2.size() < 1) {
            System.err.println(" GA error !!!!!!!! PANIC !!!!!!!!!!!:\n\t\t No of processors are not correct !! \n\t\tChromosome 1 has " + duplicateList.size() + " processors and Chromosome 2 has " + duplicateList2.size() + " processors.");
        }
        if (duplicateList.get(0).getTimeCounter() < duplicateList2.get(0).getTimeCounter()) {
            return chromosome1;
        } else {
            return chromosome2;
        }
        
    }
    
    private Processor getProcessorOrAnyOther(String id, ArrayList<Processor> processorsForSelection, Chromosome chromosome) {
        Processor processor = chromosome.getProcessorsHM().get(id);
        boolean notFound = true;
        while (notFound && processorsForSelection.size() > 0) {
            int remainingSlotsOnProcessor = processor.getAvailSlots() - processor.getQue().size();
            if (remainingSlotsOnProcessor == 0) {
                processorsForSelection.remove(processor);
                int randomIndex = randInt(0, processorsForSelection.size());
                processor = processorsForSelection.get(randomIndex);
                remainingSlotsOnProcessor = processor.getAvailSlots() - processor.getQue().size();
                if (remainingSlotsOnProcessor > 0) {
                    notFound = false;
                }
            } else {
                notFound = false;
            }
        }
        if (notFound && processorsForSelection.size() < 1) {
            ArrayList<Processor> otherOne = new ArrayList<>();
            otherOne.addAll(chromosome.getProcessorsHM().values());
            otherOne.get(randInt(0, otherOne.size()));
        }
        
        return processor;
    }
    
    private void reassignProcessorsAccToSlots(Chromosome chromosome) {
        ArrayList<Processor> processorsForSelection = new ArrayList<>();
        processorsForSelection.addAll(chromosome.getProcessors());
        for (int i = 0; i < chromosome.getElements().size(); i++) {
            Task get = chromosome.getElements().get(i);
            Processor processor = getProcessorOrAnyOther(get.getParallelForLoop().getNodeUUID(), processorsForSelection, chromosome);
            get.setPretask(processor.getQue());
            processor.getQue().add(get);
            for (int j = 0; j < get.getDeplist().size(); j++) {
                Task get1 = get.getDeplist().get(j);
                processor.getDepque().add(get1);
            }
            
            long endTime = 0;
            if (!get.getDeplist().isEmpty()) {
                ArrayList<Task> duplicateList = new ArrayList<>();
                Collections.copy(duplicateList, get.getDeplist());
                Collections.sort(duplicateList, TaskComparator.END_TIME_SORT.reversed());
                endTime = duplicateList.get(0).getEndtime();
            }
            get.getParallelForLoop().setNodeUUID(processor.getId());
            get.setStarttime(endTime + processor.getTimeCounter() + processor.getDistanceFromCurrent() + 1);
            get.setEndtime(get.getStarttime() + (long) Math.ceil(get.getValue()));
            get.setExectime(get.getEndtime() - get.getStarttime());
            processor.incrementTimeCounter(get.getEndtime() + 1);
        }
    }
    
    private class Chromosome {
        
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

    /**
     * Returns a pseudo-random number between min and max, inclusive. The
     * difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum availSlots
     * @param max Maximum availSlots. Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public int randInt(int min, int max) {

        // nextInt is normally exclusive of the top availSlots,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;
        
        return randomNum;
    }
    
    private class Task {
        
        private String id;
        private double value;
        private long starttime;
        private long endtime;
        private long exectime;
        private ArrayList<Task> deplist;
        private ArrayList<Task> pretask;
        private ParallelForSENP parallelForLoop;
        
        public Task(String id, double value, long starttime, long endtime, long exectime, ArrayList<Task> deplist, ArrayList<Task> pretask) {
            this.id = id;
            this.value = value;
            this.starttime = starttime;
            this.endtime = endtime;
            this.deplist = deplist;
            this.pretask = pretask;
            this.exectime = exectime;
        }
        
        public Task(Task otherTask) {
            otherTask.id = this.id;
            otherTask.value = this.value;
            otherTask.starttime = this.starttime;
            otherTask.endtime = this.endtime;
            otherTask.deplist = this.deplist;
            otherTask.pretask = this.pretask;
            otherTask.exectime = this.exectime;
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
        
        public ArrayList<Task> getPretask() {
            return pretask;
        }
        
        public void setPretask(ArrayList<Task> value) {
            this.pretask = value;
        }
        
        public ParallelForSENP getParallelForLoop() {
            return parallelForLoop;
        }
        
        public void setParallelForLoop(ParallelForSENP parallelForLoop) {
            this.parallelForLoop = parallelForLoop;
        }
        
        @Override
        public String toString() {
            return "Task{" + "id=" + id + ", value=" + value + ", starttime=" + starttime + ", endtime=" + endtime + ", exectime=" + exectime + ", deplist=" + deplist + ", pretask=" + pretask + ", parallelForLoop=" + parallelForLoop + '}';
        }
        
    }
    
    private class Processor {
        
        private String id;
        private int availSlots;
        private double CPUScore;
        private double performanceFactor;
        private ArrayList<Task> que;
        private ArrayList<Task> depque;
        private long timeCounter = 0;
        private long distanceFromCurrent = 0;
        
        public Processor(String id, int availSlots, double cpuScore, ArrayList<Task> que, ArrayList<Task> depque, long distanceFromCurrent, double performanceFactor) {
            this.id = id;
            this.availSlots = availSlots;
            this.CPUScore = cpuScore;
            this.que = que;
            this.depque = depque;
            this.distanceFromCurrent = distanceFromCurrent;
            this.performanceFactor = performanceFactor;
        }
        
        public Processor(Processor otherProcessor) {
            otherProcessor.id = this.id;
            otherProcessor.availSlots = this.availSlots;
            otherProcessor.CPUScore = this.CPUScore;
            otherProcessor.que = this.que;
            otherProcessor.depque = this.depque;
            otherProcessor.distanceFromCurrent = distanceFromCurrent;
            otherProcessor.performanceFactor = performanceFactor;
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
        
        public ArrayList<Task> getDepque() {
            return depque;
        }
        
        public void setDepque(ArrayList<Task> value) {
            this.depque = value;
        }
        
        public double getCPUScore() {
            return CPUScore;
        }
        
        public void setCPUScore(double CPUScore) {
            this.CPUScore = CPUScore;
        }
        
        public long getTimeCounter() {
            return timeCounter;
        }
        
        public long incrementTimeCounter(long delta) {
            return timeCounter += delta;
        }
        
        public void setTimeCounter(long timeCounter) {
            this.timeCounter = timeCounter;
        }
        
        public long getDistanceFromCurrent() {
            return distanceFromCurrent;
        }
        
        public void setDistanceFromCurrent(long distanceFromCurrent) {
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
                return Long.valueOf(o1.getDistanceFromCurrent()).compareTo(o2.getDistanceFromCurrent());
            }
        }, AVAIL_SLOTS_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Integer.valueOf(o1.getAvailSlots()).compareTo(o2.getAvailSlots());
            }
        }, TIME_COUNTER_SORT {
            @Override
            public int compare(Processor o1, Processor o2) {
                return Long.valueOf(o1.getTimeCounter()).compareTo(o2.getTimeCounter());
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
        }, PRE_TASK_SIZE_SORT {
            @Override
            public int compare(Task o1, Task o2) {
                return Integer.valueOf(o1.getPretask().size()).compareTo(o2.getPretask().size());
            }
        }, DEP_LIST_SIZE_SORT {
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
