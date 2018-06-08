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
import in.co.s13.sips.lib.common.datastructure.Node;
import in.co.s13.sips.lib.common.datastructure.ParallelForLoop;
import in.co.s13.sips.scheduler.Scheduler;
import in.co.s13.sips.schedulers.lib.ga.Chromosome;
import in.co.s13.sips.schedulers.lib.ga.FreeSlot;
import in.co.s13.sips.schedulers.lib.ga.Processor;
import in.co.s13.sips.schedulers.lib.ga.Task;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

/**
 *
 * @author nika
 */
public class GATDS implements Scheduler {

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

    @Override
    public ArrayList<SIPSTask> schedule(ConcurrentHashMap<String, Node> nodes, ConcurrentHashMap<String, SIPSTask> tasks, JSONObject schedulerSettings) {
        GA geneticAlgorithm = new GA();
        Chromosome bestChromosome = geneticAlgorithm.getBestChromosomeForTasks(nodes, tasks, schedulerSettings);
        geneticAlgorithm.getOutputs().forEach((t) -> {
            outputs.add(t);
        });
        geneticAlgorithm.getErrors().forEach((t) -> {
            errors.add(t);
        });
        outputs.add("GA RESULT :");

        bestChromosome.getProcessors().forEach((element) -> {
            outputs.add("Processor :" + element.getId());
            element.getQue().forEach((value) -> {
                value.getSipsTask().setStartTime(value.getStarttime());
                outputs.add("\t Task: " + value.getId() + " start: " + value.getStarttime() + " end: " + value.getEndtime() + " time: " + value.getExectime() + " problemsize: " + value.getProblemSize());
            });

        });

        duplicateTasks(bestChromosome);
        ArrayList<SIPSTask> result2 = new ArrayList<>();
        outputs.add("After Duplication :");
        bestChromosome.getProcessors().forEach((element) -> {
            outputs.add("Processor :" + element.getId());
            element.getQue().forEach((value) -> {
                outputs.add("\t Task: " + value.getId() + " start: " + value.getStarttime() + " end: " + value.getEndtime() + " time: " + value.getExectime() + " problemsize: " + value.getProblemSize());
                value.getSipsTask().setStartTime(value.getStarttime());
                result2.add(value.getSipsTask());
            });

        });
        backupNodes.addAll(geneticAlgorithm.getBackupNodes());
        this.totalChunks = (int) result2.stream().filter(value -> value.isDuplicate() == false).count();
        this.selectedNodes = (int) bestChromosome.getProcessors().stream().filter(p -> p.getQue().size() > 0).count();
        this.nodes = bestChromosome.getProcessors().size();
        Collections.sort(result2, SIPSTask.SIPSTaskComparator.START_TIME);
        return result2;
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        GA geneticAlgorithm = new GA();
        Chromosome bestChromosome = geneticAlgorithm.getBestChromosome(liveNodes, loop, schedulerSettings);
        geneticAlgorithm.getOutputs().forEach((t) -> {
            outputs.add(t);
        });
        geneticAlgorithm.getErrors().forEach((t) -> {
            errors.add(t);
        });
        duplicateTasks(bestChromosome);
        ArrayList<ParallelForSENP> result2 = new ArrayList<>();
        bestChromosome.getProcessors().forEach((element) -> {
            outputs.add("Processor :");
            element.getQue().forEach((value) -> {
                outputs.add("\t Task: " + value.getId() + " start: " + value.getStarttime() + " end: " + value.getEndtime() + " time: " + value.getExectime() + " problemsize: " + value.getProblemSize());
                result2.add(value.getParallelForLoop());
            });

        });
        backupNodes.addAll(geneticAlgorithm.getBackupNodes());
        this.totalChunks = (int) result2.stream().filter(value -> value.isDuplicate() == false).count();
        this.selectedNodes = (int) bestChromosome.getProcessors().stream().filter(p -> p.getQue().size() > 0).count();
        this.nodes = bestChromosome.getProcessors().size();
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

    private int getTaskLocationInQueue(Processor processor, long endTime) {
        ArrayList<Task> queue = processor.getQue();
        for (int i = 0; i < queue.size(); i++) {
            Task get = queue.get(i);
            if (endTime == get.getEndtime()) {
                return i + 1;
            }
        }
        return 0;
    }

    private int getMaxMinDepTime(ArrayList<Processor> processors, Task task) {
        long depMax = 0, depMin = 0;
        ArrayList<String> depQueue = task.getDeplist();
        for (int i = 0; i < depQueue.size(); i++) {
            String dependency = depQueue.get(i);
            depMin = 0;
            for (int k = 0; k < processors.size(); k++) {
                Processor processor = processors.get(k);

                ArrayList<Task> queue = processor.getQue();
                for (int j = 0; j < queue.size(); j++) {
                    Task task1 = queue.get(j);
                    if (dependency == null ? task1.getId() == null : dependency.equals(task1.getId())) {
                        if (depMin == 0 && depMin < task1.getEndtime()) {
                            depMin = task1.getEndtime();
                        } else if (depMin > 0 && depMin > task1.getEndtime()) {
                            depMin = task1.getEndtime();
                        }

                    }
                }
            }
            if (depMax < depMin) {
                depMax = depMin;
            }
        }

        return 0;
    }

    private long getMinEndtimeOfDepTask(ArrayList<Processor> processor, String id) {
        long min = 0;
        for (int j = 0; j < processor.size(); j++) {
            Processor get = processor.get(j);
            ArrayList<Task> tasks = get.getQue();
            for (int i = 0; i < tasks.size(); i++) {
                Task get1 = tasks.get(i);
                if (get1.getId().equalsIgnoreCase(id)) {
                    if (min == 0 && get1.getEndtime() > min) {
                        min = get1.getEndtime();
                    } else if (min > 0 && get1.getEndtime() < min) {
                        min = get1.getEndtime();

                    }
                }
            }

        }

        return min;
    }

    private Task getDepTaskwMinEndtime(ArrayList<Processor> processor, String id) {
        long min = 0;
        Task depTask = null;
        for (int j = 0; j < processor.size(); j++) {
            Processor get = processor.get(j);
            ArrayList<Task> tasks = get.getQue();
            for (int i = 0; i < tasks.size(); i++) {
                Task get1 = tasks.get(i);
                if (get1.getId().equalsIgnoreCase(id)) {
                    if (min == 0 && get1.getEndtime() > min) {
                        min = get1.getEndtime();
                        depTask = get1;
                    } else if (min > 0 && get1.getEndtime() < min) {
                        min = get1.getEndtime();
                        depTask = get1;

                    }
                }
            }

        }

        return depTask;
    }

    private void duplicateTasks(Chromosome chromosome) {
        outputs.add("Duplicating Tasks");
        ArrayList<Processor> processors = new ArrayList<>();
        processors.addAll(chromosome.getProcessorsHM().values());
        Collections.sort(processors, Processor.ProcessorComparator.TIME_COUNTER_SORT.reversed().thenComparing(Processor.ProcessorComparator.DISTANCE_SORT).thenComparing(Processor.ProcessorComparator.PF_SORT));
        long maxTimeCounter = 0;
        if (!processors.isEmpty()) {
            maxTimeCounter = processors.get(0).getTimeCounter();
        }
        for (int i = 0; i < chromosome.getElements().size(); i++) {
            Task get = chromosome.getElements().get(i);
            for (int j = 0; j < get.getDeplist().size(); j++) {
                outputs.add("Looking for duplicable dependency");
                String dependencyTaskId = get.getDeplist().get(j);
                Task dependencyTask = getDepTaskwMinEndtime(processors, dependencyTaskId);
                if (dependencyTask != null) {
                    long depEndTime = dependencyTask.getEndtime();
                    Collections.sort(processors, Processor.ProcessorComparator.FREE_SLOTS_SORT.reversed().thenComparing(Processor.ProcessorComparator.DISTANCE_SORT).thenComparing(Processor.ProcessorComparator.PF_SORT));
                    int k = 0;
                    boolean eligible = false;
                    while ((!eligible) && k < processors.size()) {
                        Processor processor = processors.get(k);
                        ArrayList<FreeSlot> freeSlots = processor.getFreeSlots();
                        for (int l = 0; l < freeSlots.size(); l++) {
                            FreeSlot freeSlot = freeSlots.get(l);
                            long estimateExcTime = (long) (dependencyTask.getProblemSize() * processor.getPerformanceFactor());
                            if (freeSlot.getSize() >= estimateExcTime
                                    && freeSlot.getFrom() >= getMaxMinDepTime(processors, dependencyTask)
                                    && depEndTime >= freeSlot.getFrom() + estimateExcTime + (processor.getDistanceFromCurrent())
                                    && dependencyTask.getExectime() >= estimateExcTime + (processor.getDistanceFromCurrent())
                                    && !dependencyTask.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                                Task duplicate = new Task(dependencyTask.getId(), estimateExcTime, freeSlot.getFrom() + processor.getDistanceFromCurrent(), freeSlot.getFrom() + estimateExcTime, estimateExcTime, new ArrayList<String>(), dependencyTask.getProblemSize());// dependencyTask.getParallelForLoop()
                                outputs.add("\tAlready Allocated to " + get.getNodeUUID() + " Now Allocating to " + processor.getId());

                                if (dependencyTask.getParallelForLoop() != null) {
                                    ParallelForSENP duplicateSENP = new ParallelForSENP(dependencyTask.getParallelForLoop());
                                    duplicateSENP.setNodeUUID(processor.getId());
                                    dependencyTask.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
                                    duplicateSENP.setDuplicateOf(dependencyTask.getParallelForLoop().getNodeUUID() + "-" + dependencyTask.getParallelForLoop().getChunkNo());
                                    duplicate.setParallelForLoop(duplicateSENP);
                                    processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getFrom()), duplicate);
                                    eligible = true;
                                } else if (dependencyTask.getSipsTask() != null) {
                                    SIPSTask duplicateTask = new SIPSTask(dependencyTask.getSipsTask());
                                    duplicateTask.setNodeUUID(processor.getId());
                                    duplicateTask.setStartTime(duplicate.getStarttime());
                                    dependencyTask.getSipsTask().addDuplicate(duplicateTask.getNodeUUID() + "-" + duplicateTask.getId());
                                    duplicateTask.setDuplicateOf(dependencyTask.getSipsTask().getNodeUUID() + "-" + dependencyTask.getSipsTask().getId());
                                    duplicate.setSipsTask(duplicateTask);
                                    processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getFrom()), duplicate);
                                    eligible = true;
                                }
                                break;
                            }
                        }

                        if (!eligible && processor.getTimeCounter() < maxTimeCounter && !get.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                            long estimateExcTime = (long) (dependencyTask.getProblemSize() * processor.getPerformanceFactor());
                            if (maxTimeCounter - processor.getTimeCounter() >= estimateExcTime
                                    && processor.getTimeCounter() >= getMaxMinDepTime(processors, dependencyTask)
                                    && depEndTime >= processor.getTimeCounter() + estimateExcTime + processor.getDistanceFromCurrent()
                                    && dependencyTask.getExectime() >= estimateExcTime + processor.getDistanceFromCurrent()
                                    && !dependencyTask.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                                Task duplicate = new Task(dependencyTask.getId(), estimateExcTime, processor.getTimeCounter() + processor.getDistanceFromCurrent(), processor.getTimeCounter() + estimateExcTime, estimateExcTime, new ArrayList<String>(), dependencyTask.getProblemSize());
                                if (dependencyTask.getParallelForLoop() != null) {
                                    ParallelForSENP duplicateSENP = new ParallelForSENP(dependencyTask.getParallelForLoop());
                                    duplicateSENP.setNodeUUID(processor.getId());
                                    dependencyTask.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
                                    duplicateSENP.setDuplicateOf(dependencyTask.getParallelForLoop().getNodeUUID() + "-" + dependencyTask.getParallelForLoop().getChunkNo());
                                    duplicate.setParallelForLoop(duplicateSENP);
                                    processor.getQue().add(getTaskLocationInQueue(processor, processor.getTimeCounter()), duplicate);
                                    eligible = true;
                                } else if (dependencyTask.getSipsTask() != null) {
                                    SIPSTask duplicateTask = new SIPSTask(dependencyTask.getSipsTask());
                                    duplicateTask.setNodeUUID(processor.getId());
                                    duplicateTask.setStartTime(duplicate.getStarttime());
                                    dependencyTask.getSipsTask().addDuplicate(duplicateTask.getNodeUUID() + "-" + duplicateTask.getId());
                                    duplicateTask.setDuplicateOf(dependencyTask.getSipsTask().getNodeUUID() + "-" + dependencyTask.getSipsTask().getId());
                                    duplicate.setSipsTask(duplicateTask);
                                    processor.getQue().add(getTaskLocationInQueue(processor, processor.getTimeCounter()), duplicate);
                                    eligible = true;
                                }
                            }
                        }
                        k++;
                    }
                }
            }
            int k = 0;
            boolean eligible = false;
            outputs.add("Looking for duplicable task " + get.getId());

            while ((!eligible) && k < processors.size()) {
                outputs.add("Checking out processor number " + k);
                Processor processor = processors.get(k);
                ArrayList<FreeSlot> freeSlots = processor.getFreeSlots();

                for (int l = 0; l < freeSlots.size(); l++) {
                    outputs.add("Checking out slot number " + l);
                    FreeSlot freeSlot = freeSlots.get(l);
                    long estimateExcTime = (long) (get.getProblemSize() * processor.getPerformanceFactor());
                    if (freeSlot.getSize() >= estimateExcTime
                            && freeSlot.getFrom() >= getMaxMinDepTime(processors, get)
                            && get.getEndtime() >= freeSlot.getFrom() + estimateExcTime + processor.getDistanceFromCurrent()
                            && get.getExectime() >= estimateExcTime + processor.getDistanceFromCurrent()
                            && !get.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                        outputs.add("Selected slot number " + l);
                        outputs.add("\tAlready Allocated to " + get.getNodeUUID() + " Now Allocating to " + processor.getId());

                        Task duplicate = new Task(get.getId(), estimateExcTime, freeSlot.getFrom() + processor.getDistanceFromCurrent(), freeSlot.getFrom() + estimateExcTime, estimateExcTime, new ArrayList<String>(), get.getProblemSize());
                        if (get.getParallelForLoop() != null) {
                            ParallelForSENP duplicateSENP = new ParallelForSENP(get.getParallelForLoop());
                            duplicateSENP.setNodeUUID(processor.getId());
                            get.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
                            duplicateSENP.setDuplicateOf(get.getParallelForLoop().getNodeUUID() + "-" + get.getParallelForLoop().getChunkNo());
                            duplicate.setParallelForLoop(duplicateSENP);
                            processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getFrom()), duplicate);
                            eligible = true;
                        } else if (get.getSipsTask() != null) {
                            SIPSTask duplicateTask = new SIPSTask(get.getSipsTask());
                            duplicateTask.setNodeUUID(processor.getId());
                            duplicateTask.setStartTime(duplicate.getStarttime());
                            get.getSipsTask().addDuplicate(duplicateTask.getNodeUUID() + "-" + duplicateTask.getId());
                            duplicateTask.setDuplicateOf(get.getSipsTask().getNodeUUID() + "-" + get.getSipsTask().getId());
                            duplicate.setSipsTask(duplicateTask);
                            processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getFrom()), duplicate);
                            eligible = true;
                        }
                        break;
                    }
                }
                if (!eligible && processor.getTimeCounter() < maxTimeCounter && !get.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                    long estimateExcTime = (long) (get.getProblemSize() * processor.getPerformanceFactor());
                    if (maxTimeCounter - processor.getTimeCounter() >= estimateExcTime
                            && processor.getTimeCounter() >= getMaxMinDepTime(processors, get)
                            && get.getEndtime() >= processor.getTimeCounter() + estimateExcTime + processor.getDistanceFromCurrent()
                            && get.getExectime() >= estimateExcTime + processor.getDistanceFromCurrent()
                            && !get.getNodeUUID().equalsIgnoreCase(processor.getId())) {
                        Task duplicate = new Task(get.getId(), estimateExcTime, processor.getTimeCounter() + processor.getDistanceFromCurrent(), processor.getTimeCounter() + estimateExcTime, estimateExcTime, new ArrayList<String>(), get.getProblemSize());
                        if (duplicate.getParallelForLoop() != null) {
                            ParallelForSENP duplicateSENP = new ParallelForSENP(get.getParallelForLoop());
                            duplicateSENP.setNodeUUID(processor.getId());
                            get.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
                            duplicateSENP.setDuplicateOf(get.getParallelForLoop().getNodeUUID() + "-" + get.getParallelForLoop().getChunkNo());
                            duplicate.setParallelForLoop(duplicateSENP);
                            processor.getQue().add(getTaskLocationInQueue(processor, processor.getTimeCounter()), duplicate);
                            eligible = true;
                        } else if (duplicate.getSipsTask() != null) {
                            SIPSTask duplicateTask = new SIPSTask(get.getSipsTask());
                            duplicateTask.setNodeUUID(processor.getId());
                            duplicateTask.setStartTime(duplicate.getStarttime());
                            get.getSipsTask().addDuplicate(duplicateTask.getNodeUUID() + "-" + duplicateTask.getId());
                            duplicateTask.setDuplicateOf(get.getNodeUUID() + "-" + get.getSipsTask().getId());
                            duplicate.setSipsTask(duplicateTask);
                            processor.getQue().add(getTaskLocationInQueue(processor, processor.getTimeCounter()), duplicate);
                            eligible = true;

                        }
                    }
                }
                k++;
            }

        }
    }

}
