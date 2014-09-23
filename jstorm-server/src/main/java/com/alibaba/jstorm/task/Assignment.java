package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

/**
 * Assignment of one Toplogy, stored in /ZK-DIR/assignments/{topologyid}
 * nodeHost {supervisorid: hostname} -- assigned supervisor Map
 * taskStartTimeSecs: {taskid, taskStartSeconds} masterCodeDir: topology source
 * code's dir in Nimbus taskToResource: {taskid, ResourceAssignment}
 * 
 * @author Lixin/Longda
 */
public class Assignment implements Serializable {

	private static final long serialVersionUID = 6087667851333314069L;

	private final String masterCodeDir;
	/**
	 * @@@ nodeHost store <supervisorId, hostname>, this will waste some zk
	 *     storage
	 */
	private final Map<String, String> nodeHost;
	private final Map<Integer, Integer> taskStartTimeSecs;
	private final Set<ResourceWorkerSlot> workers;

	public Assignment(String masterCodeDir, Set<ResourceWorkerSlot> workers,
			Map<String, String> nodeHost,
			Map<Integer, Integer> taskStartTimeSecs) {
		this.workers = workers;
		this.nodeHost = nodeHost;
		this.taskStartTimeSecs = taskStartTimeSecs;
		this.masterCodeDir = masterCodeDir;
	}

	public Map<String, String> getNodeHost() {
		return nodeHost;
	}

	public Map<Integer, Integer> getTaskStartTimeSecs() {
		return taskStartTimeSecs;
	}

	public String getMasterCodeDir() {
		return masterCodeDir;
	}

	public Set<ResourceWorkerSlot> getWorkers() {
		return workers;
	}

	/**
	 * find workers for every supervisorId (node)
	 * 
	 * @param supervisorId
	 * @return Map<Integer, WorkerSlot>
	 */
	public Map<Integer, ResourceWorkerSlot> getTaskToNodePortbyNode(
			String supervisorId) {

		Map<Integer, ResourceWorkerSlot> result = new HashMap<Integer, ResourceWorkerSlot>();
		for (ResourceWorkerSlot worker : workers) {
			if (worker.getNodeId().equals(supervisorId)) {
				result.put(worker.getPort(), worker);
			}
		}
		return result;
	}
	
	public Set<Integer> getCurrentSuperviosrTasks(String supervisorId) {
		Set<Integer> Tasks = new HashSet<Integer>();
		
		for (ResourceWorkerSlot worker : workers) {
			if (worker.getNodeId().equals(supervisorId))
				Tasks.addAll(worker.getTasks());
		}
		
		return Tasks;
	}
	
	public Set<Integer> getCurrentSuperviosrWorkers(String supervisorId) {
		Set<Integer> workerSet = new HashSet<Integer>();
		
		for (ResourceWorkerSlot worker : workers) {
			if (worker.getNodeId().equals(supervisorId))
				workerSet.add(worker.getPort());
		}
		
		return workerSet;
	}

	public Set<Integer> getCurrentWorkerTasks(String supervisorId, int port) {

		for (ResourceWorkerSlot worker : workers) {
			if (worker.getNodeId().equals(supervisorId)
					&& worker.getPort() == port)
				return worker.getTasks();
		}

		return new HashSet<Integer>();
	}
	
	public ResourceWorkerSlot getWorkerByTaskId(Integer taskId) {
		for (ResourceWorkerSlot worker : workers) {
			if (worker.getTasks().contains(taskId))
				return worker;
		}
		return null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((masterCodeDir == null) ? 0 : masterCodeDir.hashCode());
		result = prime * result
				+ ((nodeHost == null) ? 0 : nodeHost.hashCode());
		result = prime
				* result
				+ ((taskStartTimeSecs == null) ? 0 : taskStartTimeSecs
						.hashCode());
		result = prime * result + ((workers == null) ? 0 : workers.hashCode());
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
		Assignment other = (Assignment) obj;
		if (masterCodeDir == null) {
			if (other.masterCodeDir != null)
				return false;
		} else if (!masterCodeDir.equals(other.masterCodeDir))
			return false;
		if (nodeHost == null) {
			if (other.nodeHost != null)
				return false;
		} else if (!nodeHost.equals(other.nodeHost))
			return false;
		if (taskStartTimeSecs == null) {
			if (other.taskStartTimeSecs != null)
				return false;
		} else if (!taskStartTimeSecs.equals(other.taskStartTimeSecs))
			return false;
		if (workers == null) {
			if (other.workers != null)
				return false;
		} else if (!workers.equals(other.workers))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
