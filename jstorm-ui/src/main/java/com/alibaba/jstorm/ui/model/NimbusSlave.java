package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.alibaba.jstorm.utils.NetWorkUtils;

public class NimbusSlave implements Serializable {
	
	private static final long serialVersionUID = 2134152872653314400L;

	private String ip;
	
	private String hostname;
	
	private String uptime;
	
	public NimbusSlave(String hostname, String uptime) {
		this.hostname = hostname;
		this.uptime = uptime;
		String[] fields = StringUtils.split(hostname, ":");
		this.ip = NetWorkUtils.host2Ip(fields[0]);
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
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
