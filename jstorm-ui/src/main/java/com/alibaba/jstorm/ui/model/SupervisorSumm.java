package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import backtype.storm.generated.SupervisorSummary;

import com.alibaba.jstorm.common.stats.StatBuckets;

/**
 * mainpage:SupervisorSummary
 *
 * @author xin.zhou
 *
 */
public class SupervisorSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247850L;
	private String host;
    private String uptime;
    private String totalCpu;
    private String usedCpu;
    private String totalMem;
    private String usedMem;
    private String totalDisk;
    private String usedDisk;
    private String totalPort;
    private String usedPort;

    public SupervisorSumm() {
    }
    
    public SupervisorSumm(SupervisorSummary s) {
        this.host = s.get_host();
        this.uptime= StatBuckets.prettyUptimeStr(s.get_uptime_secs());
        
        this.totalCpu = String.valueOf(s.get_num_cpu());
        this.usedCpu = String.valueOf(s.get_num_used_cpu());
        
        this.totalMem = String.valueOf(s.get_num_mem());
        this.usedMem = String.valueOf(s.get_num_used_mem());
        
        this.totalDisk = String.valueOf(s.get_num_disk());
        this.usedDisk = String.valueOf(s.get_num_used_disk());
        
        this.totalPort = String.valueOf(s.get_num_workers());
        this.usedPort = String.valueOf(s.get_num_used_workers());
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getTotalCpu() {
        return totalCpu;
    }

    public void setTotalCpu(String totalCpu) {
        this.totalCpu = totalCpu;
    }

    public String getUsedCpu() {
        return usedCpu;
    }

    public void setUsedCpu(String usedCpu) {
        this.usedCpu = usedCpu;
    }

    public String getTotalMem() {
        return totalMem;
    }

    public void setTotalMem(String totalMem) {
        this.totalMem = totalMem;
    }

    public String getUsedMem() {
        return usedMem;
    }

    public void setUsedMem(String usedMem) {
        this.usedMem = usedMem;
    }

    public String getTotalDisk() {
        return totalDisk;
    }

    public void setTotalDisk(String totalDisk) {
        this.totalDisk = totalDisk;
    }

    public String getUsedDisk() {
        return usedDisk;
    }

    public void setUsedDisk(String usedDisk) {
        this.usedDisk = usedDisk;
    }

    public String getTotalPort() {
        return totalPort;
    }

    public void setTotalPort(String totalPort) {
        this.totalPort = totalPort;
    }

    public String getUsedPort() {
        return usedPort;
    }

    public void setUsedPort(String usedPort) {
        this.usedPort = usedPort;
    }

    
}
