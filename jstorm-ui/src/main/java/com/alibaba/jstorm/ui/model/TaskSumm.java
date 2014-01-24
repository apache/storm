package com.alibaba.jstorm.ui.model;

/**
 * taskpage:TaskSummary
 * 
 * @author xin.zhou
 * 
 */
public class TaskSumm {

	private String taskid;
	private String host;
	private String port;
	private String topologyname;
	private String componentId;
	private String uptime;

	public TaskSumm(String taskid, String host, String port,
			String topologyname, String componentId, String uptime) {
		this.taskid = taskid;
		this.host = host;
		this.port = port;
		this.topologyname = topologyname;
		this.componentId = componentId;
		this.uptime = uptime;

	}

	public TaskSumm(String taskid, String host, String port, String uptime) {
		this.taskid = taskid;
		this.host = host;
		this.port = port;
		this.uptime = uptime;

	}

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

	public String getTopologyname() {
		return topologyname;
	}

	public void setTopologyname(String topologyname) {
		this.topologyname = topologyname;
	}

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}
}
