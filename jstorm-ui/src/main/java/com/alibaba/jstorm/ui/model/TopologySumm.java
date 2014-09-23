package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

/**
 * mainpage:TopologySummary
 * 
 * @author xin.zhou
 * 
 */
public class TopologySumm implements Serializable {

	private static final long serialVersionUID = 189495975527682322L;
	private String topologyName;
	private String topologyId;
	private String status;
	private String uptime;
	private String numWorkers;
	private String numTasks;
	private String errorInfo;

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	public String getTopologyId() {
		return topologyId;
	}

	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
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

	public String getNumWorkers() {
		return numWorkers;
	}

	public void setNumWorkers(String numWorkers) {
		this.numWorkers = numWorkers;
	}

	public String getNumTasks() {
		return numTasks;
	}

	public void setNumTasks(String numTasks) {
		this.numTasks = numTasks;
	}
	
	public String getErrorInfo() {
		return this.errorInfo;
	}
	
	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}

}
