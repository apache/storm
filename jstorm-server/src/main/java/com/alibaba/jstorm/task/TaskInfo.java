package com.alibaba.jstorm.task;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * /storm-zk-root/tasks/{topologyid}/{taskid} data
 */
public class TaskInfo implements Serializable {

	private static final long serialVersionUID = 5625165079055837777L;
	private String componentId;

	public TaskInfo(String componentId) {
		this.componentId = componentId;
	}

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	@Override
	public boolean equals(Object assignment) {
		if (assignment instanceof TaskInfo
				&& ((TaskInfo) assignment).getComponentId().equals(
						getComponentId())) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.getComponentId().hashCode();
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
