package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import backtype.storm.generated.SupervisorSummary;

import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * mainpage:SupervisorSummary
 * 
 * @author xin.zhou
 * 
 */
public class SupervisorSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247850L;
	private String ip;

	private String host;
	private String uptime;
	private String totalPort;
	private String usedPort;

	public SupervisorSumm() {
	}

	public SupervisorSumm(SupervisorSummary s) {
		this.host = NetWorkUtils.ip2Host(s.get_host());
		this.ip = NetWorkUtils.host2Ip(s.get_host());
		this.uptime = StatBuckets.prettyUptimeStr(s.get_uptime_secs());

		this.totalPort = String.valueOf(s.get_num_workers());
		this.usedPort = String.valueOf(s.get_num_used_workers());
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

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
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
