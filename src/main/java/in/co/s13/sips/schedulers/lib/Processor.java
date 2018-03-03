/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package in.co.s13.sips.schedulers.lib;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *
 * @author navde
 */
public class Processor implements Serializable {

    private String id;
    private double performance;

    private ArrayList<Task> taskQueue;
    private ArrayList<String> depque;
    private int queueLimit,queueWait;

    public Processor(String id, double prf,int queLimit,int qWait) {
        this.id = id;
        this.performance = prf;
        this.queueLimit=queLimit;
        this.queueWait=qWait;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getPerformance() {
        return performance;
    }

    public void setPerformance(double performance) {
        this.performance = performance;
    }

    public int getQueueLimit() {
        return queueLimit;
    }

    public void setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
    }

    public ArrayList<Task> getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(ArrayList<Task> value) {
        this.taskQueue = value;
    }

    public ArrayList<String> getDepque() {
        return depque;
    }

    public void setDepque(ArrayList<String> depque) {
        this.depque = depque;
    }

    @Override
    public String toString() {
        return "** Id of Processor=" + id + " with performance=" + performance + " **";
    }


}
