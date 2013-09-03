package com.alipay.dw.jstorm.ui.model;

/**
 * mainpage:ClusterSummary
 * @author xin.zhou
 *
 */
import java.io.Serializable;
public class ClusterSumm implements Serializable{
	private static final long serialVersionUID = -7936384220562644886L;
	private String nimbusuptime;
       private String supervisors;
       private String usedSlots;
       private String freeSlots;
       private String totalSlots;
       private String runningTasks;
       
       public ClusterSumm(String nimbusuptime
	                     , String supervisors
	                     , String usedSlots
	                     , String freeSlots
	                     , String totalSlots
	                     , String runningTasks){
	   this.nimbusuptime = nimbusuptime;
	   this.supervisors = supervisors;
	   this.usedSlots = usedSlots;
	   this.freeSlots = freeSlots;
	   this.totalSlots = totalSlots;
	   this.runningTasks = runningTasks;
       }

    public String getNimbusuptime() {
        return nimbusuptime;
    }

    public void setNimbusuptime(String nimbusuptime) {
        this.nimbusuptime = nimbusuptime;
    }

    public String getSupervisors() {
        return supervisors;
    }

    public void setSupervisors(String supervisors) {
        this.supervisors = supervisors;
    }

    public String getUsedSlots() {
        return usedSlots;
    }

    public void setUsedSlots(String usedSlots) {
        this.usedSlots = usedSlots;
    }

    public String getFreeSlots() {
        return freeSlots;
    }

    public void setFreeSlots(String freeSlots) {
        this.freeSlots = freeSlots;
    }

    public String getTotalSlots() {
        return totalSlots;
    }

    public void setTotalSlots(String totalSlots) {
        this.totalSlots = totalSlots;
    }

    public String getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(String runningTasks) {
        this.runningTasks = runningTasks;
    }
}
