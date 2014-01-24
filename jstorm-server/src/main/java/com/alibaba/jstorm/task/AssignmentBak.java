package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class AssignmentBak implements Serializable {

	/**  */
	private static final long serialVersionUID = 7633746649144483965L;

	private final Map<String, List<Integer>> componentTasks;
	private final Assignment assignment;

	public AssignmentBak(Map<String, List<Integer>> componentTasks,
			Assignment assignment) {
		super();
		this.componentTasks = componentTasks;
		this.assignment = assignment;
	}

	public Map<String, List<Integer>> getComponentTasks() {
		return componentTasks;
	}

	public Assignment getAssignment() {
		return assignment;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
