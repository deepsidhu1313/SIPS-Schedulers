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
    public int getSelectedNodes() {
        return selectedNodes;
    }

    private ArrayList<SIPSTask> sortTasksAccordingToDependencies(ConcurrentHashMap<String, SIPSTask> tasks) {
        ArrayList<SIPSTask> tasksList = new ArrayList<>(tasks.values());
//        outputs.add("UnSorted Tasks:" + tasksList);
        Collections.sort(tasksList, SIPSTask.SIPSTaskComparator.NO_OF_DEPENDENCIES.thenComparing(SIPSTask.SIPSTaskComparator.ID));
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            outputs.add("\n\n\n\nTask:" + get.getId());
            ArrayList<String> deps = get.getDependsOn();
            for (int j = 0; j < deps.size(); j++) {
                String get1 = deps.get(j);
                SIPSTask dep = tasks.get(get1);
                outputs.add("\nDep:" + dep.getId());
                int depCurrentIndex = getTaskIndex(tasksList, dep);
                int tasksCurrentIndex = getTaskIndex(tasksList, get);
                if (depCurrentIndex > tasksCurrentIndex) {
                    outputs.add("Task:" + get.getId() + " Dep:" + dep.getId() + " TaskLoc:" + tasksCurrentIndex + " DepLoc:" + depCurrentIndex);
                    outputs.add("Before Move Tasks:" + tasksList);
                    tasksList.remove(depCurrentIndex);
                    tasksList.add(tasksCurrentIndex, dep);
                    outputs.add("After Move Tasks:" + tasksList);
                    i = 0;
                }
            }
        }
        return tasksList;
    }

    private int getTaskIndex(ArrayList<SIPSTask> tasksList, SIPSTask task) {
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            if (get.getName().equalsIgnoreCase(task.getName())) {
                return i;
            }
        }
        return 0;
    }

    @Override
    public ArrayList<SIPSTask> schedule(ConcurrentHashMap<String, Node> nodes, ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {
        Chromosome resultant = this.getBestChromosomeForTasks(nodes, tasks, schedulerSettings);
        ArrayList<SIPSTask> result2 = new ArrayList<>();
        resultant.getElements().forEach(element -> result2.add(element.getSipsTask()));
        Collections.sort(result2, SIPSTask.SIPSTaskComparator.START_TIME);
        return result2;
    }

    private int getTaskIdByName(ArrayList<SIPSTask> tasksList, String name) {
        for (int i = 0; i < tasksList.size(); i++) {
            SIPSTask get = tasksList.get(i);
            if (get.getName().equalsIgnoreCase(name)) {
                return get.getId();
            }
        }
        return -1;
    }

    private void addTaskDependecies(Chromosome chromosome, ArrayList<SIPSTask> tasksList) {
        ArrayList<Task> elements = chromosome.getElements();
        for (int i = 0; i < elements.size(); i++) {
            Task get = elements.get(i);
            System.out.println("Setting dependency for Task " + get.getId());
            if (get.getSipsTask() != null) {
                System.out.println("SIPS Task is not Null");
                System.out.println("SIPS Task Depends On " + get.getSipsTask().getDependsOn());
                ArrayList<String> sipsTaskDepList = get.getSipsTask().getDependsOn();
                for (int j = 0; j < sipsTaskDepList.size(); j++) {
                    String get1 = sipsTaskDepList.get(j);
                    int id = getTaskIdByName(tasksList, get1);
                    System.out.println("Id of Task : " + get1 + " is " + id);
                    get.getDeplist().add("" + id);

                }
            }
        }
    }

    public Chromosome getBestChromosomeForTasks(ConcurrentHashMap<String, Node> liveNodes, ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {
        ArrayList<SIPSTask> result = sortTasksAccordingToDependencies(tasks);
        System.out.println("Sorted Tasks According to Dependecies");
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(liveNodes.values());
//        outputs.add("Before Sorting:" + nodes);

        /**
         ** Selection
         */
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.CPU_AVG_LOAD).thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
//        outputs.add("After Sorting:" + nodes);
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

        Chromosome resultant = new Chromosome();
        ArrayList<ProcessorComparator> comparators = new ArrayList<>();
        comparators.add(ProcessorComparator.AVAIL_SLOTS_SORT);
        comparators.add(ProcessorComparator.CPU_SCORE_SORT);
        comparators.add(ProcessorComparator.DISTANCE_SORT);
        comparators.add(ProcessorComparator.PF_SORT);
        ArrayList<Chromosome> chromosomes = new ArrayList<>();

        for (int r = 0; r < maxPopulation; r++) {
            outputs.add("Generating chromosome for " + r);

            ArrayList<Task> elements = new ArrayList<>();
            ArrayList<Processor> processorsForSelection = new ArrayList<>();
            processors.values().forEach(value -> processorsForSelection.add(new Processor(value)));
            ProcessorComparator randomComparator = comparators.get(randInt(0, comparators.size() - 1));
            if (randomComparator == ProcessorComparator.DISTANCE_SORT || randomComparator == ProcessorComparator.PF_SORT) {
                Collections.sort(processorsForSelection, randomComparator.reversed());
            } else {
                Collections.sort(processorsForSelection, randomComparator.reversed());
            }
            outputs.add("Sorted processors using " + randomComparator.name());
            Chromosome chromosome = new Chromosome();
            chromosome.getProcessors().addAll(processorsForSelection);
            outputs.add("Added processors to Chromosome");
            for (int j = 0; j < result.size(); j++) {
                Processor randomlySelectedNode = getRandomProcessor(processorsForSelection, processors);
                SIPSTask get = result.get(j);

                Task ga_task = new Task("" + get.getId(), (get.getLength().doubleValue() * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), get.getLength().longValue());
                SIPSTask duplicate_task = new SIPSTask(get);
                duplicate_task.setNodeUUID(randomlySelectedNode.getId());
                ga_task.setSipsTask(duplicate_task);
                elements.add(ga_task);

            }
            chromosome.getElements().addAll(elements);
            chromosomes.add(chromosome);
        }

        Chromosome chromosome1 = chromosomes.get(randInt(0, chromosomes.size() - 1));

        for (int i = 0; i < maxGenerations; i++) {
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
                outputs.add("\n\nCrossover at :" + randomCrossOverPoint + " on list of size: " + chromosome1.getElements().size());
                List<Task> first = chromosome1.getElements().subList(0, randomCrossOverPoint);
//                outputs.add("\nSublist 1:" + first);
                List<Task> second = chromosome1.getElements().subList(randomCrossOverPoint, chromosome1.getElements().size());
//                outputs.add("\nSublist 2:" + second);
                List<Task> third = chromosome2.getElements().subList(0, randomCrossOverPoint);
//                outputs.add("\nSublist 3:" + third);
                List<Task> fourth = chromosome2.getElements().subList(randomCrossOverPoint, chromosome2.getElements().size());
//                outputs.add("\nSublist 4:" + fourth);
                ArrayList<Task> temp1 = new ArrayList<>();
                temp1.addAll(first);
                temp1.addAll(fourth);
                ArrayList<Task> temp2 = new ArrayList<>();
                temp2.addAll(third);
                temp2.addAll(second);
                chromosome1.setElements(temp1);
                chromosome2.setElements(temp2);
                outputs.add("\n!!!!!Crossover Complete!!!!\n");
                chromosome1.getProcessors().clear();
                chromosome2.getProcessors().clear();
                outputs.add("\n!!!!!Cleared Processors!!!!\n");
                ArrayList<Processor> processorsForSelection = new ArrayList<>();
                ArrayList<Processor> processorsForSelection2 = new ArrayList<>();
                processors.values().forEach((value) -> {
                    processorsForSelection.add(new Processor(value));
                    processorsForSelection2.add(new Processor(value));
                });
                chromosome1.setProcessors(processorsForSelection);
                chromosome2.setProcessors(processorsForSelection2);
                outputs.add("\n!!!!!Added Processors!!!!\n");
                chromosome1.addElementsToHashMap();
                chromosome2.addElementsToHashMap();
                outputs.add("\n!!!!!Added Processors to HashMaps!!!!\n");
                System.out.println("" + " \n\nand 2nd one " + chromosome2);
                System.out.println("\nChromosomes are " + chromosome1);

                System.out.println("Adding Task Dependecies");
                addTaskDependecies(chromosome1,new ArrayList<>(tasks.values()));
                addTaskDependecies(chromosome2,new ArrayList<>(tasks.values()));
                System.out.println("Reassigning Task acc to slots");
                reassignProcessorsAccToSlots(chromosome1);
                reassignProcessorsAccToSlots(chromosome2);

//                outputs.add("" + " \n\nand 2nd one " + chromosome2);
//                outputs.add("\nChromosomes are " + chromosome1);
            }

            /**
             * Select best for mutation
             */
            outputs.add("Begining mutation");
            chromosome1 = bestForMutation(chromosome1, chromosome2);
//            outputs.add(" Selected for mutation " + chromosome1);
            /**
             * *Mutation**
             */
            int randomPointForMutation = randInt(0, chromosome1.getElements().size() - 1);
            outputs.add("Replacing " + randomPointForMutation + " with better one");
            Task task4Mutation = chromosome1.getElements().get(randomPointForMutation);
            String idOfProcessorToBeReplaced = task4Mutation.getNodeUUID();
            Processor toBeReplaced = chromosome1.getProcessorsHM().get(idOfProcessorToBeReplaced);
            ArrayList<Processor> processorsList = chromosome1.getProcessors();
            Collections.sort(processorsList, ProcessorComparator.CPU_SCORE_SORT.reversed());
            int randomIndexOfProcessor = randInt(0, processorsList.indexOf(toBeReplaced));
            outputs.add("Selected Processor " + randomIndexOfProcessor + " for Mutation");
            task4Mutation.getSipsTask().setNodeUUID(processorsList.get(randomIndexOfProcessor).getId());
            resultant = chromosome1;
        }
        reassignProcessorsAccToSlots(resultant);
//        outputs.add("Best Chromosome:" + resultant);

        this.totalChunks = resultant.getElements().size();
        this.selectedNodes = (int) resultant.getProcessors().stream().filter(p -> p.getQue().size() > 0).count();
        this.nodes = resultant.getProcessors().size();
        backupNodes.addAll(nodes);
        return resultant;
    }

    public Chromosome getBestChromosome(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        ArrayList<ParallelForSENP> result = new ArrayList<>();
        ArrayList<Node> nodes = new ArrayList<>();
        nodes.addAll(liveNodes.values());
//        outputs.add("Before Sorting:" + nodes);

        /**
         ** Selection
         */
        // first sort score in decending order, then distance in ascending order
        Collections.sort(nodes, LiveNode.LiveNodeComparator.QWAIT.thenComparing(LiveNode.LiveNodeComparator.CPU_AVG_LOAD).thenComparing(LiveNode.LiveNodeComparator.QLEN.reversed()).thenComparing(LiveNode.LiveNodeComparator.CPU_COMPOSITE_SCORE.reversed()).thenComparing(LiveNode.LiveNodeComparator.DISTANCE_FROM_CURRENT));
//        outputs.add("After Sorting:" + nodes);
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
        outputs.add("Max Score: " + maxCPUScore + " Min Expected Time:" + minExpectedTime);

        byte diff_byte = 0, chunkFactor_byte;
        short diff_short = 0, chunkFactor_short;
        int diff_int = 0, chunkFactor_int;
        long diff_long = 0, chunkFactor_long;
        float diff_float = 0, chunkFactor_float;
        double diff_double = 0, chunkFactor_double;

        boolean chunksCreated = false;
        TSS tss = new TSS();
        result = tss.scheduleParallelFor(liveNodes, loop, schedulerSettings);
        outputs.add("Intial Task Generation :" + result);
        Chromosome resultant = new Chromosome();
        ArrayList<ProcessorComparator> comparators = new ArrayList<>();
        comparators.add(ProcessorComparator.AVAIL_SLOTS_SORT);
        comparators.add(ProcessorComparator.CPU_SCORE_SORT);
        comparators.add(ProcessorComparator.DISTANCE_SORT);
        comparators.add(ProcessorComparator.PF_SORT);
        ArrayList<Chromosome> chromosomes = new ArrayList<>();

        for (int r = 0; r < maxPopulation; r++) {
            outputs.add("Generating chromosome for " + r);
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
            outputs.add("Sorted processors using " + randomComparator.name());
            Chromosome chromosome = new Chromosome();
            chromosome.getProcessors().addAll(processorsForSelection);
            outputs.add("Added processors to Chromosome");
            for (int j = 0; j < result.size(); j++) {
                Processor randomlySelectedNode = getRandomProcessor(processorsForSelection, processors);
//                outputs.add("Selected Random Processor " + randomlySelectedNode);
                ParallelForSENP get = result.get(j);
                switch (loop.getDataType()) {
                    case 0:
                        chunkFactor_byte = Byte.parseByte(get.getDiff());
                        Task task_byte = new Task("" + j, (chunkFactor_byte * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_byte);
                        ParallelForSENP duplicate_byte = new ParallelForSENP(get);
                        duplicate_byte.setNodeUUID(randomlySelectedNode.getId());
                        task_byte.setParallelForLoop(duplicate_byte);
                        elements.add(task_byte);
                        break;
                    case 1:
                        chunkFactor_short = Short.parseShort(get.getDiff());
                        Task task_short = new Task("" + j, (chunkFactor_short * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_short);
                        ParallelForSENP duplicate_short = new ParallelForSENP(get);
                        duplicate_short.setNodeUUID(randomlySelectedNode.getId());
                        task_short.setParallelForLoop(duplicate_short);
                        elements.add(task_short);
                        break;
                    case 2:
                        chunkFactor_int = Integer.parseInt(get.getDiff());
                        Task task_int = new Task("" + j, (chunkFactor_int * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_int);
                        ParallelForSENP duplicate_int = new ParallelForSENP(get);
                        duplicate_int.setNodeUUID(randomlySelectedNode.getId());
                        task_int.setParallelForLoop(duplicate_int);
                        elements.add(task_int);
                        break;
                    case 3:
                        chunkFactor_long = Long.parseLong(get.getDiff());
                        Task task_long = new Task("" + j, (chunkFactor_long * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_long);
                        ParallelForSENP duplicate_long = new ParallelForSENP(get);
                        duplicate_long.setNodeUUID(randomlySelectedNode.getId());
                        task_long.setParallelForLoop(duplicate_long);

                        elements.add(task_long);
                        break;
                    case 4:
                        chunkFactor_float = Float.parseFloat(get.getDiff());
                        Task task_float = new Task("" + j, (chunkFactor_float * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_float);
                        ParallelForSENP duplicate_float = new ParallelForSENP(get);
                        duplicate_float.setNodeUUID(randomlySelectedNode.getId());
                        task_float.setParallelForLoop(duplicate_float);
                        elements.add(task_float);
                        break;
                    case 5:
                        chunkFactor_double = Double.parseDouble(get.getDiff());
                        Task task_double = new Task("" + j, (chunkFactor_double * randomlySelectedNode.getPerformanceFactor()), 0, 0, 0, new ArrayList<String>(), chunkFactor_double);
                        ParallelForSENP duplicate_double = new ParallelForSENP(get);
                        duplicate_double.setNodeUUID(randomlySelectedNode.getId());
                        task_double.setParallelForLoop(duplicate_double);
                        elements.add(task_double);
                        break;
                }

            }
            outputs.add("Elements:" + elements);
            chromosome.getElements().addAll(elements);
            chromosomes.add(chromosome);
        }

        Chromosome chromosome1 = chromosomes.get(randInt(0, chromosomes.size() - 1));

        for (int i = 0; i < maxGenerations; i++) {
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
                outputs.add("\n\nCrossover at :" + randomCrossOverPoint + " on list of size: " + chromosome1.getElements().size());
                List<Task> first = chromosome1.getElements().subList(0, randomCrossOverPoint);
//                outputs.add("\nSublist 1:" + first);
                List<Task> second = chromosome1.getElements().subList(randomCrossOverPoint, chromosome1.getElements().size());
//                outputs.add("\nSublist 2:" + second);
                List<Task> third = chromosome2.getElements().subList(0, randomCrossOverPoint);
//                outputs.add("\nSublist 3:" + third);
                List<Task> fourth = chromosome2.getElements().subList(randomCrossOverPoint, chromosome2.getElements().size());
//                outputs.add("\nSublist 4:" + fourth);
                ArrayList<Task> temp1 = new ArrayList<>();
                temp1.addAll(first);
                temp1.addAll(fourth);
                ArrayList<Task> temp2 = new ArrayList<>();
                temp2.addAll(third);
                temp2.addAll(second);
                chromosome1.setElements(temp1);
                chromosome2.setElements(temp2);
                outputs.add("\n!!!!!Crossover Complete!!!!\n");
                chromosome1.getProcessors().clear();
                chromosome2.getProcessors().clear();
                outputs.add("\n!!!!!Cleared Processors!!!!\n");
                ArrayList<Processor> processorsForSelection = new ArrayList<>();
                ArrayList<Processor> processorsForSelection2 = new ArrayList<>();
                processors.values().forEach((value) -> {
                    processorsForSelection.add(new Processor(value));
                    processorsForSelection2.add(new Processor(value));
                });
                chromosome1.setProcessors(processorsForSelection);
                chromosome2.setProcessors(processorsForSelection2);
                outputs.add("\n!!!!!Added Processors!!!!\n");
                chromosome1.addElementsToHashMap();
                chromosome2.addElementsToHashMap();
                outputs.add("\n!!!!!Added Processors to HashMaps!!!!\n");
                reassignProcessorsAccToSlots(chromosome1);
                reassignProcessorsAccToSlots(chromosome2);

//                outputs.add("" + " \n\nand 2nd one " + chromosome2);
//                outputs.add("\nChromosomes are " + chromosome1);
            }

            /**
             * Select best for mutation
             */
            outputs.add("Begining mutation");
            chromosome1 = bestForMutation(chromosome1, chromosome2);
            outputs.add(" Selected for mutation " + chromosome1);
            /**
             * *Mutation**
             */
            int randomPointForMutation = randInt(0, chromosome1.getElements().size() - 1);
            outputs.add("Replacing " + randomPointForMutation + " with better one");
            Task task4Mutation = chromosome1.getElements().get(randomPointForMutation);
            String idOfProcessorToBeReplaced = task4Mutation.getNodeUUID();
            Processor toBeReplaced = chromosome1.getProcessorsHM().get(idOfProcessorToBeReplaced);
            ArrayList<Processor> processorsList = chromosome1.getProcessors();
            Collections.sort(processorsList, ProcessorComparator.CPU_SCORE_SORT.reversed());
            int randomIndexOfProcessor = randInt(0, processorsList.indexOf(toBeReplaced));
            outputs.add("Selected Processor " + randomIndexOfProcessor + " for Mutation");
            if (task4Mutation.getParallelForLoop() != null) {
                task4Mutation.getParallelForLoop().setNodeUUID(processorsList.get(randomIndexOfProcessor).getId());
            } else if (task4Mutation.getSipsTask() != null) {
                task4Mutation.getSipsTask().setNodeUUID(processorsList.get(randomIndexOfProcessor).getId());
            }
            resultant = chromosome1;
        }
        reassignProcessorsAccToSlots(resultant);
//        outputs.add("Best Chromosome:" + resultant);
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
            outputs.add("Choosing " + randomSelectedIndex + " processor ");
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
            outputs.add("Added processors to another array list as no slots available on selected processors");
            return otherOne.get(randInt(0, otherOne.size() - 1));
        }
    }

    private Chromosome bestForMutation(Chromosome chromosome1, Chromosome chromosome2) {
        ArrayList<Processor> duplicateList = new ArrayList<>();
//        outputs.add("Processors in 1st Chromosome " + chromosome1.getElementsHM().size());
        duplicateList.addAll(chromosome1.getProcessorsHM().values());
        Collections.sort(duplicateList, ProcessorComparator.TIME_COUNTER_SORT.reversed());
        ArrayList<Processor> duplicateList2 = new ArrayList<>();
//        outputs.add("Processors in 2nd Chromosome " + chromosome2.getProcessorsHM().size());
        duplicateList2.addAll(chromosome2.getProcessorsHM().values());
        Collections.sort(duplicateList2, ProcessorComparator.TIME_COUNTER_SORT.reversed());
        if (duplicateList.size() < 1 || duplicateList2.size() < 1) {
            errors.add(" GA error !!!!!!!! PANIC !!!!!!!!!!!:\n\t\t No of processors are not correct !! \n\t\tChromosome 1 has " + duplicateList.size() + " processors and Chromosome 2 has " + duplicateList2.size() + " processors.");
        }
        if (duplicateList.get(0).getTimeCounter() < duplicateList2.get(0).getTimeCounter()) {
            return chromosome1;
        } else {
            return chromosome2;
        }

    }

    private PairCPHM getProcessorOrAnyOther(String id, ArrayList<Processor> processorsForSelection, Chromosome chromosome) {
//        outputs.add(" \n\n\n in getter Processors " + processorsForSelection);
        Processor processor = chromosome.getProcessorsHM().get(id);

        boolean notFound = true;
        while (notFound && processorsForSelection.size() > 1) {
//            outputs.add("Looking for processor in the list");
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
//        outputs.add("Returning " + pairCPHM);
        return pairCPHM;
    }

    private long getMinEndtimeOfDepTask(ArrayList<Task> tasks, String id) {
        long min = 0;
        for (int i = 0; i < tasks.size(); i++) {
            Task get = tasks.get(i);
            if (get.getId().equalsIgnoreCase(id)) {
                if (min == 0 && get.getEndtime() > min) {
                    min = get.getEndtime();
                } else if (min > 0 && get.getEndtime() < min) {
                    min = get.getEndtime();

                }
            }
        }
        return min;
    }

    private long getMaxEndTimeofDepTask(ArrayList<String> deps, ArrayList<Task> listToSearch) {
        long max = 0;
        for (int i = 0; i < deps.size(); i++) {
            String get = deps.get(i);
            long time = getMinEndtimeOfDepTask(listToSearch, get);
            if (time > max) {
                max = time;
            }
        }
        return max;
    }

    private String getIdofDepTaskwMaxEndTime(ArrayList<String> deps, ArrayList<Task> listToSearch) {
        long max = 0;
        String id = "";
        for (int i = 0; i < deps.size(); i++) {
            String get = deps.get(i);
            long time = getMinEndtimeOfDepTask(listToSearch, get);
            if (time > max) {
                max = time;
                id = get;
            }
        }
        return id;
    }

    private Task getTaskById(ArrayList<Task> listToSearch, String id) {
        for (int i = 0; i < listToSearch.size(); i++) {
            Task get = listToSearch.get(i);
            if (get.getId().equalsIgnoreCase(id)) {
                return get;
            }
        }
        return null;
    }

    private void reassignProcessorsAccToSlots(Chromosome chromosome) {
        System.out.println("Resetting Processors");
        chromosome.getProcessors().forEach(processor -> {
            processor.getQue().clear();
            processor.setTimeCounter(0);
        });
        System.out.println("Resetted Processors");

        ArrayList<Processor> processorsForSelection = new ArrayList<>();
        processorsForSelection.addAll(chromosome.getProcessors());
        System.out.println("Readded Processors");

        for (int i = 0; i < chromosome.getElements().size(); i++) {
            System.out.println("Reassigning task " + i);
            outputs.add("Reassigning task " + i);
            Task get = chromosome.getElements().get(i);
            PairCPHM pairCPHM = getProcessorOrAnyOther(get.getNodeUUID(), processorsForSelection, chromosome);
            Processor processor = pairCPHM.getProcessor();
            System.out.println("Received Processor");
            chromosome = pairCPHM.getChromosome();
            System.out.println("Received Chromosome");
            processorsForSelection = pairCPHM.getProcessors();
            System.out.println("Received Processors");
            processor.getQue().add(get);
            System.out.println("Adding Task to processor Queue");

            processor.getDepque().addAll(get.getDeplist());
            System.out.println("Adding Task to processor Dep Queue");

            long endTime = 0;
            long depProblemSize = 0;
            if (!get.getDeplist().isEmpty()) {
                System.out.println("Deplist Not Empty");
                System.out.println("Deplist is :"+get.getDeplist());
//                ArrayList<String> duplicateList = new ArrayList<>();
//                Collections.copy(duplicateList, get.getDeplist());
//                System.out.println("\n\tDepList:" + duplicateList + "\n");
//                outputs.add("\n\tDepList:" + duplicateList + "\n");
                endTime = getMaxEndTimeofDepTask(get.getDeplist(), chromosome.getElements());
                System.out.println("\nDepEndTime:" + endTime + "\n");

                depProblemSize = (long) getTaskById(chromosome.getElements(), getIdofDepTaskwMaxEndTime(get.getDeplist(), chromosome.getElements())).getProblemSize();
            }
            outputs.add("Before:\nProcessor Time Counter: " + processor.getTimeCounter() + " distance:" + processor.getDistanceFromCurrent() + " DepEndTime:" + endTime + " DepProblemSize:" + depProblemSize);

            System.out.println("Before:\nProcessor Time Counter: " + processor.getTimeCounter() + " distance:" + processor.getDistanceFromCurrent() + " DepEndTime:" + endTime + " DepProblemSize:" + depProblemSize);
            get.setStarttime(endTime + processor.getTimeCounter() + (processor.getDistanceFromCurrent() * (depProblemSize)) + 1);
            get.setValue(get.getProblemSize() * processor.getPerformanceFactor());
            get.setEndtime(get.getStarttime() + (long) Math.ceil(get.getValue()));
            get.setExectime(get.getEndtime() - get.getStarttime());
            if (get.getParallelForLoop() != null) {
                get.getParallelForLoop().setNodeUUID(processor.getId());
            } else if (get.getSipsTask() != null) {
                get.getSipsTask().setNodeUUID(processor.getId());
                get.getSipsTask().setStartTime(get.getStarttime());
            }
            processor.incrementTimeCounter(get.getExectime() + 1);
            outputs.add("Task " + i + "  " + get.toString() + " end at " + processor.getTimeCounter() + " distance:" + processor.getDistanceFromCurrent());
            System.out.println("Task " + i + "  " + get.toString() + " end at " + processor.getTimeCounter() + " distance:" + processor.getDistanceFromCurrent());
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
