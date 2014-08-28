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

}
