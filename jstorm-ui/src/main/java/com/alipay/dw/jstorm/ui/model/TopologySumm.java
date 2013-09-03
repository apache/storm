package com.alipay.dw.jstorm.ui.model;

import java.io.Serializable;

/**
 * mainpage:TopologySummary
 * 
 * @author xin.zhou
 * 
 */
public class TopologySumm implements Serializable {

	private static final long serialVersionUID = 189495975527682322L;
	private String topologyname;
    private String topologyid;
    private String status;
    private String uptime;
    private String numworkers;
    private String numtasks;
    private int    luptime;
    
    public TopologySumm(String topologyname, String topologyid, String status,
            String uptime, String numworkers, String numtasks, int luptime) {
        this.topologyname = topologyname;
        this.topologyid = topologyid;
        this.status = status;
        this.uptime = uptime;
        this.numworkers = numworkers;
        this.numtasks = numtasks;
        this.luptime = luptime;
    }
    
    public String getTopologyname() {
        return topologyname;
    }
    
    public void setTopologyname(String topologyname) {
        this.topologyname = topologyname;
    }
    
    public String getTopologyid() {
        return topologyid;
    }
    
    public void setTopologyid(String topologyid) {
        this.topologyid = topologyid;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getUptime() {
        return uptime;
    }
    
    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
    
    public String getNumworkers() {
        return numworkers;
    }
    
    public void setNumworkers(String numworkers) {
        this.numworkers = numworkers;
    }
    
    public String getNumtasks() {
        return numtasks;
    }
    
    public void setNumtasks(String numtasks) {
        this.numtasks = numtasks;
    }
    
    public int getLuptime() {
        return luptime;
    }
    
    public void setLuptime(int luptime) {
        this.luptime = luptime;
    }
}
