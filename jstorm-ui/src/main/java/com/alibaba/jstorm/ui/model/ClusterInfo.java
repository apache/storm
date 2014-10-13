package com.alibaba.jstorm.ui.model;

import java.util.List;
import java.io.Serializable;

public class ClusterInfo implements Serializable {
	private static final long serialVersionUID = -7966384220162644896L;
	
	private String clusterName;
	private String zkRoot;
	private Integer zkPort;
	private List<String> zkServers;

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String name) {
		this.clusterName = name;
	}

	public String getZkRoot() {
		return zkRoot;
	}

	public void setZkRoot(String zkRoot) {
		this.zkRoot = zkRoot;
	}
	
	public Integer getZkPort() {
		return zkPort;
	}

	public void setZkPort(Integer zkPort) {
		this.zkPort = zkPort;
	}
	
	public List<String> getZkServers() {
		return zkServers;
	}

	public void setZkServers(List<String> zkServers) {
		this.zkServers = zkServers;
	}
}
