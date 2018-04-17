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

    @Override
    public int getSelectedNodes() {
        return selectedNodes;
    }

    @Override
    public ArrayList<TaskNodePair> schedule() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ArrayList<ParallelForSENP> scheduleParallelFor(ConcurrentHashMap<String, Node> liveNodes, ParallelForLoop loop, JSONObject schedulerSettings) {
        GA geneticAlgorithm = new GA();
        Chromosome bestChromosome = geneticAlgorithm.getBestChromosome(liveNodes, loop, schedulerSettings);
        duplicateTasks(bestChromosome);
        ArrayList<ParallelForSENP> result2 = new ArrayList<>();
        bestChromosome.getProcessors().forEach((element) -> {
//            System.out.println("Processor :");
            element.getQue().forEach((value) -> {
//                System.out.println("\t Task: " + value.getId() + " start: " + value.getStarttime() + " end: " + value.getEndtime() + " time: " + value.getExectime() + " problemsize: " + value.getProblemSize());
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

    private int getTaskLocationInQueue(Processor processor, long startTime) {
        ArrayList<Task> queue = processor.getQue();
        for (int i = 0; i < queue.size(); i++) {
            Task get = queue.get(i);
            if (startTime == get.getStarttime()) {
                return i;
            }
        }
        return 0;
    }

    private void duplicateTasks(Chromosome chromosome) {
//        System.out.println("Duplicating Tasks");
        ArrayList<Processor> processors = new ArrayList<>();
        processors.addAll(chromosome.getProcessorsHM().values());
        for (int i = 0; i < chromosome.getElements().size(); i++) {
            Task get = chromosome.getElements().get(i);
            for (int j = 0; j < get.getDeplist().size(); j++) {
//                System.out.println("Looking for duplicable dependency");
                Task dependencyTask = get.getDeplist().get(j);
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
                        if (freeSlot.getSize() > estimateExcTime
                                && depEndTime > freeSlot.getFrom() + estimateExcTime + processor.getDistanceFromCurrent()
                                && dependencyTask.getExectime() > estimateExcTime + processor.getDistanceFromCurrent()
                                && !dependencyTask.getParallelForLoop().getNodeUUID().equalsIgnoreCase(processor.getId())) {
                            Task duplicate = new Task(dependencyTask.getId(), estimateExcTime, freeSlot.getFrom() + processor.getDistanceFromCurrent(), freeSlot.getFrom() + estimateExcTime, estimateExcTime, new ArrayList<Task>(), dependencyTask.getProblemSize());// dependencyTask.getParallelForLoop()
                            ParallelForSENP duplicateSENP = new ParallelForSENP(dependencyTask.getParallelForLoop());
                            duplicateSENP.setNodeUUID(processor.getId());
                            dependencyTask.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
                            duplicateSENP.setDuplicateOf(dependencyTask.getParallelForLoop().getNodeUUID() + "-" + dependencyTask.getParallelForLoop().getChunkNo());
                            duplicate.setParallelForLoop(duplicateSENP);
                            processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getTo()), duplicate);
                            eligible = true;
                        }
                    }
                    k++;
                }
            }
            int k = 0;
            boolean eligible = false;
            System.out.println("Looking for duplicable task " + get.getId());

            while ((!eligible) && k < processors.size()) {
//                System.out.println("Checking out processor number " + k);
                Processor processor = processors.get(k);
                ArrayList<FreeSlot> freeSlots = processor.getFreeSlots();
//                freeSlots.stream().forEach(value -> System.out.println(value));

                for (int l = 0; l < freeSlots.size(); l++) {
//                    System.out.println("Checking out slot number " + l);
                    FreeSlot freeSlot = freeSlots.get(l);
                    long estimateExcTime = (long) (get.getProblemSize() * processor.getPerformanceFactor());
                    if (freeSlot.getSize() > estimateExcTime
                            && get.getEndtime() > freeSlot.getFrom() + estimateExcTime + processor.getDistanceFromCurrent()
                            && get.getExectime() > estimateExcTime + processor.getDistanceFromCurrent()
                            && !get.getParallelForLoop().getNodeUUID().equalsIgnoreCase(processor.getId())) {
//                        System.out.println("Selected slot number " + l);

                        Task duplicate = new Task(get.getId(), estimateExcTime, freeSlot.getFrom() + processor.getDistanceFromCurrent(), freeSlot.getFrom() + estimateExcTime, estimateExcTime, new ArrayList<Task>(), get.getProblemSize());
//                        System.out.println("Duplicated task");
                        ParallelForSENP duplicateSENP = new ParallelForSENP(get.getParallelForLoop());
//                        System.out.println("Duplicated SENP");
                        duplicateSENP.setNodeUUID(processor.getId());
//                        System.out.println("Setting UUID");
                        get.getParallelForLoop().addDuplicate(duplicateSENP.getNodeUUID() + "-" + duplicateSENP.getChunkNo());
//                        System.out.println("Added duplicated ID");
                        duplicateSENP.setDuplicateOf(get.getParallelForLoop().getNodeUUID() + "-" + get.getParallelForLoop().getChunkNo());
//                        System.out.println("Set deuplicate of");
                        duplicate.setParallelForLoop(duplicateSENP);
//                        System.out.println("setting modified duplicate SENP");
                        processor.getQue().add(getTaskLocationInQueue(processor, freeSlot.getTo()), duplicate);
//                        System.out.println("modified QUEUE");
                        eligible = true;
                    }
                }
                k++;
            }

        }
    }

}
