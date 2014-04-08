package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Supervisor LocalAssignment
 * 
 */
public class LocalAssignment implements Serializable {
	public static final long serialVersionUID = 4054639727225043554L;
	private final String topologyId;
	private final String topologyName;
	private Set<Integer> taskIds;
	private int memSlotNum;
	private int cpuSlotNum;

	public LocalAssignment(String topologyId, Set<Integer> taskIds, String topologyName) {
		this.topologyId = topologyId;
		this.taskIds = new HashSet<Integer>(taskIds);
		this.topologyName = topologyName;
	}

	public String getTopologyId() {
		return topologyId;
	}


	public Set<Integer> getTaskIds() {
		return taskIds;
	}

	public void setTaskIds(Set<Integer> taskIds) {
		this.taskIds = new HashSet<Integer>(taskIds);
	}

	public void addMemSlotNum(int slotNum) {
		memSlotNum += slotNum;
	}
	
	public void addCpuSlotNum(int slotCpu) {
		cpuSlotNum += slotCpu;
	}

	public int getMemSlotNum() {
		return memSlotNum;
	}
	
	public int getCpuSlotNum() {
		return cpuSlotNum;
	}
	
	public String getTopologyName() {
		return topologyName;
	}

	@Override
	public boolean equals(Object localAssignment) {
		if (localAssignment instanceof LocalAssignment
				&& ((LocalAssignment) localAssignment).getTopologyId().equals(
						topologyId)
				&& ((LocalAssignment) localAssignment).getTaskIds().equals(
						taskIds)) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.taskIds.hashCode() + this.topologyId.hashCode();
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
