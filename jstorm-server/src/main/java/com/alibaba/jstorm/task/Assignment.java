package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.resource.ResourceAssignment;

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
	private final Map<Integer, ResourceAssignment> taskToResource;

	public Assignment(String masterCodeDir,
			Map<Integer, ResourceAssignment> taskToResource,
			Map<String, String> nodeHost,
			Map<Integer, Integer> taskStartTimeSecs) {
		this.taskToResource = taskToResource;
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

	public Map<Integer, ResourceAssignment> getTaskToResource() {
		return taskToResource;
	}

	/**
	 * find taskToResource for every supervisorId (node)
	 * 
	 * @param supervisorId
	 * @return Map<Integer, WorkerSlot>
	 */
	public Map<Integer, ResourceAssignment> getTaskToResourcebyNode(
			String supervisorId) {

		Map<Integer, ResourceAssignment> taskToPortbyNode = new TreeMap<Integer, ResourceAssignment>();

		for (Entry<Integer, ResourceAssignment> entry : taskToResource
				.entrySet()) {
			String node = entry.getValue().getSupervisorId();
			if (node.equals(supervisorId)) {
				taskToPortbyNode.put(entry.getKey(), entry.getValue());
			}
		}
		return taskToPortbyNode;
	}

	public Set<Integer> getCurrentWokerTasks(String supervisorId, int port) {
		Set<Integer> ret = new TreeSet<Integer>();

		for (Entry<Integer, ResourceAssignment> entry : taskToResource
				.entrySet()) {
			Integer taskId = entry.getKey();
			ResourceAssignment resource = entry.getValue();

			if (resource.getSupervisorId().equals(supervisorId) == false) {
				continue;
			}

			if (resource.getPort() != port) {
				continue;
			}

			ret.add(taskId);
		}

		return ret;
	}

	public static Map<WorkerSlot, List<Integer>> getWorkerTasks(
			Map<Integer, ResourceAssignment> taskToResource) {
		Map<WorkerSlot, List<Integer>> ret = new TreeMap<WorkerSlot, List<Integer>>();

		for (Entry<Integer, ResourceAssignment> entry : taskToResource
				.entrySet()) {
			Integer taskId = entry.getKey();
			ResourceAssignment assignment = entry.getValue();

			WorkerSlot workerSlot = new WorkerSlot(
					assignment.getSupervisorId(), assignment.getPort());

			List<Integer> taskList = ret.get(workerSlot);
			if (taskList == null) {
				taskList = new ArrayList<Integer>();
				ret.put(workerSlot, taskList);
			}

			taskList.add(taskId);
		}

		for (Entry<WorkerSlot, List<Integer>> entry : ret.entrySet()) {
			List<Integer> list = entry.getValue();
			Collections.sort(list);
		}
		return ret;
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
		result = prime * result
				+ ((taskToResource == null) ? 0 : taskToResource.hashCode());
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
		if (taskToResource == null) {
			if (other.taskToResource != null)
				return false;
		} else if (!taskToResource.equals(other.taskToResource))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
