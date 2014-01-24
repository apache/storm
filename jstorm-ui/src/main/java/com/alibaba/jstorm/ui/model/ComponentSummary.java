package com.alibaba.jstorm.ui.model;

/**
 * componentpage:ComponentSummary
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

public class ComponentSummary implements Serializable {

	private static final long serialVersionUID = 681219575043845569L;
	private String componentId;
	private String topologyname;
	private String parallelism;
	private String cpuNum;
	private String memNum;
	private String diskNum;

	public ComponentSummary() {
	}

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public String getTopologyname() {
		return topologyname;
	}

	public void setTopologyname(String topologyname) {
		this.topologyname = topologyname;
	}

	public String getParallelism() {
		return parallelism;
	}

	public void setParallelism(String parallelism) {
		this.parallelism = parallelism;
	}

	public String getCpuNum() {
		return cpuNum;
	}

	public void setCpuNum(String cpuNum) {
		this.cpuNum = cpuNum;
	}

	public String getMemNum() {
		return memNum;
	}

	public void setMemNum(String memNum) {
		this.memNum = memNum;
	}

	public String getDiskNum() {
		return diskNum;
	}

	public void setDiskNum(String diskNum) {
		this.diskNum = diskNum;
	}

}
