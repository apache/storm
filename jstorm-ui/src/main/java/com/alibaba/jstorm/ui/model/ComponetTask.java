package com.alibaba.jstorm.ui.model;

/**
 * componentpage:Task
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

public class ComponetTask extends ComponentStats implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7396303067234858439L;
    private String taskid;
    private String host;
    private String port;
    private String uptime;
    private String lastErr;
    private String diskSlot;


    public String getTaskid() {
        return taskid;
    }

    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getLastErr() {
        return lastErr;
    }

    public void setLastErr(String lastErr) {
        this.lastErr = lastErr;
    }

    public String getDiskSlot() {
        return diskSlot;
    }

    public void setDiskSlot(String diskSlot) {
        this.diskSlot = diskSlot;
    }

}
