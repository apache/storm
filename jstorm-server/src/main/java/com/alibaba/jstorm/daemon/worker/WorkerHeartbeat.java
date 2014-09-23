package com.alibaba.jstorm.daemon.worker;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Worker's Heartbeat data woker will update the object to
 * /LOCAL-DIR/workers/${woker-id}/heartbeats
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerHeartbeat implements Serializable {

	private static final long serialVersionUID = -914166726205534892L;
	private int timeSecs;
	private String topologyId;
	private Set<Integer> taskIds;
	private Integer port;

	public WorkerHeartbeat(int timeSecs, String topologyId,
			Set<Integer> taskIds, Integer port) {

		this.timeSecs = timeSecs;
		this.topologyId = topologyId;
		this.taskIds = new HashSet<Integer>(taskIds);
		this.port = port;

	}

	public int getTimeSecs() {
		return timeSecs;
	}

	public void setTimeSecs(int timeSecs) {
		this.timeSecs = timeSecs;
	}

	public String getTopologyId() {
		return topologyId;
	}

	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
	}

	public Set<Integer> getTaskIds() {
		return taskIds;
	}

	public void setTaskIds(Set<Integer> taskIds) {
		this.taskIds = new HashSet<Integer>(taskIds);
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
