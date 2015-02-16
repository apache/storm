package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.alibaba.jstorm.utils.NetWorkUtils;

public class NimbusSlave implements Serializable {
	
	private static final long serialVersionUID = 2134152872653314400L;

	private String ip;
	
	private String logviewPort;
	
	private String hostname;
	
	private String uptime;
	
	public NimbusSlave(String hostname, String uptime, int port) {
		
		if (hostname.contains(":")) {
			String firstPart = hostname.substring(0, hostname.indexOf(":") );
			String lastPart = hostname.substring(hostname.indexOf(":"));
			this.hostname = (NetWorkUtils.ip2Host(firstPart) + lastPart);
			this.ip =  (NetWorkUtils.host2Ip(firstPart));
		} else {
			this.hostname = hostname;
			this.ip = (NetWorkUtils.host2Ip(hostname));
		}
		
		this.uptime = uptime;
		this.logviewPort = String.valueOf(port);
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

	public String getLogviewPort() {
		return logviewPort;
	}

	public void setLogviewPort(String logviewPort) {
		this.logviewPort = logviewPort;
	}
	
	
}
