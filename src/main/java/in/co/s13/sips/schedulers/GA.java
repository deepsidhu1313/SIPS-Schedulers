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
import in.co.s13.sips.schedulers.lib.ga.Chromosome;
import in.co.s13.sips.schedulers.lib.ga.PairCPHM;
import in.co.s13.sips.schedulers.lib.ga.Processor;
import in.co.s13.sips.schedulers.lib.ga.Processor.ProcessorComparator;
import in.co.s13.sips.schedulers.lib.ga.Task;
import in.co.s13.sips.schedulers.lib.ga.Task.TaskComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

/**
 *
 * @author nika
 */
public class GA implements Scheduler {
    // Usually this can be a field rather than a method variable

    Random rand = new Random();
    private int nodes, totalChunks, selectedNodes;
    private ArrayList<Node> backupNodes = new ArrayList<>();

    @Override
    public int getSelectedNodes() {
        return selectedNodes;
    }

    @Override
    public ArrayList<SIPSTask> schedule(ConcurrentHashMap<String, Node> nodes, ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public Chromosome getBestChromosome(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(liveNodes.values());
//        System.out.println("Before Sorting:" + nodes);

        /**
         ** Selection
         */
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
//        System.out.println("After Sorting:" + nodes);
        int maxNodes = schedulerSettings.getInt("MaxNodes", 4);
        int maxGenerations = schedulerSettings.getInt("MaxGenerations", 4);
        int maxPopulation = schedulerSettings.getInt("MaxPopulation", 8);
        if (maxNodes > 1) {
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
            processors.put(get.getUuid(), new Processor(get.getUuid(), availSlotsOnNode, get.getCPUScore(), new ArrayList<Task>(), new ArrayList<>(), get.getDistanceFromCurrent(), maxCPUScore / get.getCPUScore()));
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
        System.out.println("Intial Task Generation :" + result);
        i = 0;
        Chromosome resultant = new Chromosome();
        ArrayList<ProcessorComparator> comparators = new ArrayList<>();
        comparators.add(ProcessorComparator.AVAIL_SLOTS_SORT);
        comparators.add(ProcessorComparator.CPU_SCORE_SORT);
        comparators.add(ProcessorComparator.DISTANCE_SORT);
        comparators.add(ProcessorComparator.PF_SORT);
        ArrayList<Chromosome> chromosomes = new ArrayList<>();

        for (int r = 0; r < maxPopulation; r++) {
            System.out.println("Generating chromosome for " + r);
            chunkFactor_byte = 0;
            chunkFactor_short = 0;
            chunkFactor_int = 0;
            chunkFactor_long = 0;
            chunkFactor_float = 0;
            chunkFactor_double = 0;

            ArrayList<Task> elements = new ArrayList<>();
            ArrayList<Processor> processorsForSelection = new ArrayList<>();
            processors.values().forEach(value -> processorsForSelection.add(new Processor(value)));
            ProcessorComparator randomComparator = comparators.get(randInt(0, comparators.size() - 1));
            if (randomComparator == ProcessorComparator.DISTANCE_SORT || randomComparator == ProcessorComparator.PF_SORT) {
                Collections.sort(processorsForSelection, randomComparator.reversed());
            } else {
                Collections.sort(processorsForSelection, randomComparator.reversed());
            }
            System.out.println("Sorted processors using " + randomComparator.name());
            Chromosome chromosome = new Chromosome();
            chromosome.getProcessors().addAll(processorsForSelection);
            System.out.println("Added processors to Chromosome");
            for (int j = 0; j < result.size(); j++) {
                Processor randomlySelectedNode = getRandomProcessor(processorsForSelection, processors);
//                System.out.println("Selected Random Processor " + randomlySelectedNode);
                ParallelForSENP get = result.get(j);
                switch (loop.getDataType()) {
                    case 0:
                        chunkFactor_byte = Byte.parseByte(get.getDiff());
                        Task task_byte = new Task("" + j, (chunkFactor_byte * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_byte);
                        ParallelForSENP duplicate_byte = new ParallelForSENP(get);
                        duplicate_byte.setNodeUUID(randomlySelectedNode.getId());
                        task_byte.setParallelForLoop(duplicate_byte);
                        elements.add(task_byte);
                        break;
                    case 1:
                        chunkFactor_short = Short.parseShort(get.getDiff());
                        Task task_short = new Task("" + j, (chunkFactor_short * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_short);
                        ParallelForSENP duplicate_short = new ParallelForSENP(get);
                        duplicate_short.setNodeUUID(randomlySelectedNode.getId());
                        task_short.setParallelForLoop(duplicate_short);
                        elements.add(task_short);
                        break;
                    case 2:
                        chunkFactor_int = Integer.parseInt(get.getDiff());
                        Task task_int = new Task("" + j, (chunkFactor_int * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_int);
                        ParallelForSENP duplicate_int = new ParallelForSENP(get);
                        duplicate_int.setNodeUUID(randomlySelectedNode.getId());
                        task_int.setParallelForLoop(duplicate_int);
                        elements.add(task_int);
                        break;
                    case 3:
                        chunkFactor_long = Long.parseLong(get.getDiff());
                        Task task_long = new Task("" + j, (chunkFactor_long * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_long);
                        ParallelForSENP duplicate_long = new ParallelForSENP(get);
                        duplicate_long.setNodeUUID(randomlySelectedNode.getId());
                        task_long.setParallelForLoop(duplicate_long);

                        elements.add(task_long);
                        break;
                    case 4:
                        chunkFactor_float = Float.parseFloat(get.getDiff());
                        Task task_float = new Task("" + j, (chunkFactor_float * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_float);
                        ParallelForSENP duplicate_float = new ParallelForSENP(get);
                        duplicate_float.setNodeUUID(randomlySelectedNode.getId());
                        task_float.setParallelForLoop(duplicate_float);
                        elements.add(task_float);
                        break;
                    case 5:
                        chunkFactor_double = Double.parseDouble(get.getDiff());
                        Task task_double = new Task("" + j, (chunkFactor_double * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<Task>(), chunkFactor_double);
                        ParallelForSENP duplicate_double = new ParallelForSENP(get);
                        duplicate_double.setNodeUUID(randomlySelectedNode.getId());
                        task_double.setParallelForLoop(duplicate_double);
                        elements.add(task_double);
                        break;
                }

            }
            System.out.println("Elements:" + elements);
            chromosome.getElements().addAll(elements);
            chromosomes.add(chromosome);
        }

        Chromosome chromosome1 = chromosomes.get(randInt(0, chromosomes.size() - 1));

        for (i = 0; i < maxGenerations; i++) {
            /**
             * *CrossOver**
             */
            boolean failedToChoosePoint = false;
            int counter = 0;
            Chromosome chromosome2 = chromosomes.get(randInt(0, chromosomes.size() - 1));
            if (chromosome1.getElements().size() > 2 && chromosome2.getElements().size() > 2) {
                int randomCrossOverPoint = randInt(0, chromosome1.getElements().size() - 1);
                while (randomCrossOverPoint >= (chromosome1.getElements().size())) {
                    randomCrossOverPoint = randInt(0, chromosome1.getElements().size() - 1);
                    if (counter > 10) {
                        failedToChoosePoint = true;
                    }
                    counter++;
                }
                if (failedToChoosePoint) {
                    randomCrossOverPoint = ((chromosome1.getElements().size()) / 2);
                }
                System.out.println("\n\nCrossover at :" + randomCrossOverPoint + " on list of size: " + chromosome1.getElements().size());
                List<Task> first = chromosome1.getElements().subList(0, randomCrossOverPoint);
//                System.out.println("\nSublist 1:" + first);
                List<Task> second = chromosome1.getElements().subList(randomCrossOverPoint, chromosome1.getElements().size());
//                System.out.println("\nSublist 2:" + second);
                List<Task> third = chromosome2.getElements().subList(0, randomCrossOverPoint);
//                System.out.println("\nSublist 3:" + third);
                List<Task> fourth = chromosome2.getElements().subList(randomCrossOverPoint, chromosome2.getElements().size());
//                System.out.println("\nSublist 4:" + fourth);
                ArrayList<Task> temp1 = new ArrayList<>();
                temp1.addAll(first);
                temp1.addAll(fourth);
                ArrayList<Task> temp2 = new ArrayList<>();
                temp2.addAll(third);
                temp2.addAll(second);
                chromosome1.setElements(temp1);
                chromosome2.setElements(temp2);
                System.out.println("\n!!!!!Crossover Complete!!!!\n");
                chromosome1.getProcessors().clear();
                chromosome2.getProcessors().clear();
                System.out.println("\n!!!!!Cleared Processors!!!!\n");
                ArrayList<Processor> processorsForSelection = new ArrayList<>();
                ArrayList<Processor> processorsForSelection2 = new ArrayList<>();
                processors.values().forEach((value) -> {
                    processorsForSelection.add(new Processor(value));
                    processorsForSelection2.add(new Processor(value));
                });
                chromosome1.setProcessors(processorsForSelection);
                chromosome2.setProcessors(processorsForSelection2);
                System.out.println("\n!!!!!Added Processors!!!!\n");
                chromosome1.addElementsToHashMap();
                chromosome2.addElementsToHashMap();
                System.out.println("\n!!!!!Added Processors to HashMaps!!!!\n");
                reassignProcessorsAccToSlots(chromosome1);
                reassignProcessorsAccToSlots(chromosome2);

//                System.out.println("" + " \n\nand 2nd one " + chromosome2);
//                System.out.println("\nChromosomes are " + chromosome1);
            }

            /**
             * Select best for mutation
             */
            System.out.println("Begining mutation");
            chromosome1 = bestForMutation(chromosome1, chromosome2);
//            System.out.println(" Selected for mutation " + chromosome1);
            /**
             * *Mutation**
             */
            int randomPointForMutation = randInt(0, chromosome1.getElements().size() - 1);
            System.out.println("Replacing " + randomPointForMutation + " with better one");
            Task task4Mutation = chromosome1.getElements().get(randomPointForMutation);
            String idOfProcessorToBeReplaced = task4Mutation.getParallelForLoop().getNodeUUID();
            Processor toBeReplaced = chromosome1.getProcessorsHM().get(idOfProcessorToBeReplaced);
            ArrayList<Processor> processorsList = chromosome1.getProcessors();
            Collections.sort(processorsList, ProcessorComparator.CPU_SCORE_SORT.reversed());
            int randomIndexOfProcessor = randInt(0, processorsList.indexOf(toBeReplaced));
            System.out.println("Selected Processor " + randomIndexOfProcessor + " for Mutation");
            task4Mutation.getParallelForLoop().setNodeUUID(processorsList.get(randomIndexOfProcessor).getId());
            resultant = chromosome1;
        }
        reassignProcessorsAccToSlots(resultant);
//        System.out.println("Best Chromosome:" + resultant);
        this.totalChunks = resultant.getElements().size();
        this.selectedNodes = (int) resultant.getProcessors().stream().filter(p -> p.getQue().size() > 0).count();
        this.nodes = resultant.getProcessors().size();
        backupNodes.addAll(nodes);
        return resultant;
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        Chromosome resultant = this.getBestChromosome(liveNodes, loop, schedulerSettings);
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
            int randomSelectedIndex = randInt(0, processorsForSelection.size() - 1);
            System.out.println("Choosing " + randomSelectedIndex + " processor ");
            Processor random = processorsForSelection.get(randomSelectedIndex);
            random.setAvailSlots(random.getAvailSlots() - 1);
            int remainingSlotsOnProcessor = random.getAvailSlots();
            if (remainingSlotsOnProcessor < 1) {
                processorsForSelection.remove(randomSelectedIndex);
            }
            return random;
        } else {
            ArrayList<Processor> otherOne = new ArrayList<>();
            otherOne.addAll(allNodes.values());
            System.out.println("Added processors to another array list as no slots available on selected processors");
            return otherOne.get(randInt(0, otherOne.size() - 1));
        }
    }

    private Chromosome bestForMutation(Chromosome chromosome1, Chromosome chromosome2) {
        ArrayList<Processor> duplicateList = new ArrayList<>();
        System.out.println("Processors in 1st Chromosome " + chromosome1.getElementsHM().size());
        duplicateList.addAll(chromosome1.getProcessorsHM().values());
        Collections.sort(duplicateList, ProcessorComparator.TIME_COUNTER_SORT.reversed());
        ArrayList<Processor> duplicateList2 = new ArrayList<>();
        System.out.println("Processors in 2nd Chromosome " + chromosome2.getProcessorsHM().size());
        duplicateList2.addAll(chromosome2.getProcessorsHM().values());
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

    private PairCPHM getProcessorOrAnyOther(String id, ArrayList<Processor> processorsForSelection, Chromosome chromosome) {
//        System.out.println(" \n\n\n in getter Processors " + processorsForSelection);
        Processor processor = chromosome.getProcessorsHM().get(id);

        boolean notFound = true;
        while (notFound && processorsForSelection.size() > 1) {
            System.out.println("Looking for processor in the list");
            int remainingSlotsOnProcessor = processor.getAvailSlots() - processor.getQue().size();
            if (remainingSlotsOnProcessor == 0) {
                processorsForSelection.remove(processor);
                int randomIndex = randInt(0, processorsForSelection.size() - 1);
                processor = processorsForSelection.get(randomIndex);
                remainingSlotsOnProcessor = processor.getAvailSlots() - processor.getQue().size();
                if (remainingSlotsOnProcessor > 0) {
                    notFound = false;
                }
            } else {
                notFound = false;
            }
        }
        System.out.println("Finished while loop");
        if (notFound && processorsForSelection.size() <= 1) {
            System.out.println("Looking for processor in the Hashmap ");
            ArrayList<Processor> otherOne = new ArrayList<>();
            otherOne.addAll(chromosome.getProcessorsHM().values());
            System.out.println("Added values from Hashmap " + otherOne.size());
            int randomIndex = randInt(0, otherOne.size() - 1);
            System.out.println("Random Index is " + randomIndex);
            processor = otherOne.get(randomIndex);
            System.out.println("Processor with id " + id + " either not found or doesn't have any slot left Returning random processor ");

        }
        PairCPHM pairCPHM = new PairCPHM(processor, chromosome, processorsForSelection);
//        System.out.println("Returning " + pairCPHM);
        return pairCPHM;
    }

    private void reassignProcessorsAccToSlots(Chromosome chromosome) {
        chromosome.getProcessors().forEach(processor -> {
            processor.getQue().clear();
        });
        ArrayList<Processor> processorsForSelection = new ArrayList<>();
        processorsForSelection.addAll(chromosome.getProcessors());
        for (int i = 0; i < chromosome.getElements().size(); i++) {
            System.out.println("Reassigning task " + i);
            Task get = chromosome.getElements().get(i);
            PairCPHM pairCPHM = getProcessorOrAnyOther(get.getParallelForLoop().getNodeUUID(), processorsForSelection, chromosome);
            Processor processor = pairCPHM.getProcessor();
            chromosome = pairCPHM.getChromosome();
            processorsForSelection = pairCPHM.getProcessors();
            System.out.println("Added pretask to task " + i);
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
            get.setValue(get.getProblemSize() * processor.getPerformanceFactor());
            get.setEndtime(get.getStarttime() + (long) Math.ceil(get.getValue()));
            get.setExectime(get.getEndtime() - get.getStarttime());
            processor.incrementTimeCounter(get.getEndtime() + 1);
            System.out.println("Task " + i + " will end at " + processor.getTimeCounter());
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

}
