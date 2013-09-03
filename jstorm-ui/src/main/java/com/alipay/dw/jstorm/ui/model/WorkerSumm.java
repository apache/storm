package com.alipay.dw.jstorm.ui.model;

import java.io.Serializable;

import com.alipay.dw.jstorm.common.stats.StatBuckets;

import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.WorkerSummary;

/**
 * mainpage:SupervisorSummary
 * 
 * @author longda
 * 
 */
public class WorkerSumm implements Serializable {
    
    private static final long serialVersionUID = -5631649054937247856L;
    private String            port;
    private String            topology;
    private String            uptime;
    private String            tasks;
    private String            components;
    
    public WorkerSumm(String port, String topology, String uptime,
            String tasks, String components) {
        this.port = port;
        this.topology = topology;
        this.uptime = uptime;
        this.tasks = tasks;
        this.components = components;
    }
    
    public WorkerSumm(WorkerSummary workerSummary) {
        this.port = String.valueOf(workerSummary.get_port());
        this.topology = workerSummary.get_topology();
        
        StringBuilder taskSB = new StringBuilder();
        StringBuilder componentSB = new StringBuilder();
        boolean isFirst = true;
        
        int minUptime = 0;
        for (TaskSummary taskSummary : workerSummary.get_tasks()) {
            if (isFirst == false) {
                taskSB.append(',');
                componentSB.append(',');
            } else {
                minUptime = taskSummary.get_uptime_secs();
            }
            
            taskSB.append(taskSummary.get_task_id());
            componentSB.append(taskSummary.get_component_id());
            
            if (minUptime < taskSummary.get_uptime_secs()) {
                minUptime = taskSummary.get_uptime_secs();
            }
            
            isFirst = false;
        }
        
        this.uptime = StatBuckets.prettyUptimeStr(minUptime);
        this.tasks = taskSB.toString();
        this.components = componentSB.toString();
    }
    
    public String getPort() {
        return port;
    }
    
    public void setPort(String port) {
        this.port = port;
    }
    
    public String getTopology() {
        return topology;
    }
    
    public void setTopology(String topology) {
        this.topology = topology;
    }
    
    public String getUptime() {
        return uptime;
    }
    
    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
    
    public String getTasks() {
        return tasks;
    }
    
    public void setTasks(String tasks) {
        this.tasks = tasks;
    }
    
    public String getComponents() {
        return components;
    }
    
    public void setComponents(String components) {
        this.components = components;
    }
    
}
