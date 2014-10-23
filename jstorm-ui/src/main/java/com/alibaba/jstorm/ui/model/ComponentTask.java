package com.alibaba.jstorm.ui.model;

/**
 * componentpage:Task
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

public class ComponentTask extends ComponentStats implements Serializable {

	private static final long serialVersionUID = -8501129148725843924L;
	
	private String topologyid;
	private String componentid;
	private String taskid;
	private String ip;
	private String host;
	private String port;
	private String uptime;
	private String lastErr;
	private String status;

	public String getTopologyid() {
		return topologyid;
	}

	public void setTopologyid(String topologyid) {
		this.topologyid = topologyid;
	}
	
	public String getComponentid() {
		return componentid;
	}

	public void setComponentid(String componentid) {
		this.componentid = componentid;
	}
	
	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
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
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
