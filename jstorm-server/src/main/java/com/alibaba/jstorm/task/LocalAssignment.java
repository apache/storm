package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
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
	private long mem;
	private int cpu;
	private String jvm;

	public LocalAssignment(String topologyId, Set<Integer> taskIds,
			String topologyName, long mem, int cpu, String jvm) {
		this.topologyId = topologyId;
		this.taskIds = new HashSet<Integer>(taskIds);
		this.topologyName = topologyName;
		this.mem = mem;
		this.cpu = cpu;
		this.jvm = jvm;
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

	public String getTopologyName() {
		return topologyName;
	}

	public String getJvm() {
		return jvm;
	}

	public void setJvm(String jvm) {
		this.jvm = jvm;
	}

	public long getMem() {
		return mem;
	}

	public void setMem(long mem) {
		this.mem = mem;
	}

	public int getCpu() {
		return cpu;
	}

	public void setCpu(int cpu) {
		this.cpu = cpu;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cpu;
		result = prime * result + ((jvm == null) ? 0 : jvm.hashCode());
		result = prime * result + (int) (mem ^ (mem >>> 32));
		result = prime * result + ((taskIds == null) ? 0 : taskIds.hashCode());
		result = prime * result
				+ ((topologyId == null) ? 0 : topologyId.hashCode());
		result = prime * result
				+ ((topologyName == null) ? 0 : topologyName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocalAssignment other = (LocalAssignment) obj;
		if (cpu != other.cpu)
			return false;
		if (jvm == null) {
			if (other.jvm != null)
				return false;
		} else if (!jvm.equals(other.jvm))
			return false;
		if (mem != other.mem)
			return false;
		if (taskIds == null) {
			if (other.taskIds != null)
				return false;
		} else if (!taskIds.equals(other.taskIds))
			return false;
		if (topologyId == null) {
			if (other.topologyId != null)
				return false;
		} else if (!topologyId.equals(other.topologyId))
			return false;
		if (topologyName == null) {
			if (other.topologyName != null)
				return false;
		} else if (!topologyName.equals(other.topologyName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
