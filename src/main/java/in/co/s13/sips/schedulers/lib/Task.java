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
public class Task<T> implements Serializable {

    private String id;
    private double value;
    private double starttime;
    private double endtime;
    private double exectime;
    private ArrayList<String> deplist;
    private ArrayList<String> pretask;
    private T task;
    public Task(String id, float value, double starttime, double endtime,double exectime, ArrayList<String> deplist, ArrayList<String> pretask) {
        this.id = id;
        this.value = value;
        this.starttime = starttime;
        this.endtime = endtime;
        this.deplist = deplist;
        this.pretask = pretask;
        this.exectime=exectime;
    }

    public T getTask() {
        return task;
    }

    public void setTask(T task) {
        this.task = task;
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

    public double getStarttime() {
        return starttime;
    }

    public void setStarttime(double starttime) {
        this.starttime = starttime;
    }

    public double getEndtime() {
        return endtime;
    }

    public void setEndtime(double endtime) {
        this.endtime = endtime;
    }

    public double getExectime() {
        return exectime;
    }

    public void setExectime(double exectime) {
        this.exectime = exectime;
    }

    public ArrayList<String> getDeplist() {
        return deplist;
    }

    public void setDeplist(ArrayList<String> deplist) {
        this.deplist = deplist;
    }

    public ArrayList<String> getPretask() {
        return pretask;
    }

    public void setPretask(ArrayList<String> pretask) {
        this.pretask = pretask;
    }

    public boolean isIndependent() {
        return deplist.isEmpty();
    }

    
    @Override
    public String toString() {
        return "** Id of Task=" + id + ",  value=" + value + ", Depends On Tasks =" + deplist + ", preTasks are : " + pretask +" StartTime:"+starttime+ " Endtime:"+endtime+" ExecTime:"+exectime+" **";
    }



}
