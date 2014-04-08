package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

public class NimbusSlave implements Serializable {
	
	private static final long serialVersionUID = 2134152872653314400L;

	private String hostname;
	
	private String uptime;
	
	public NimbusSlave(String hostname, String uptime) {
		this.hostname = hostname;
		this.uptime = uptime;
	}
	
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public String getUptime() {
		return uptime;
	}
	public void setUptime(String uptime) {
		this.uptime = uptime;
	}
	
}
