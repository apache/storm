package com.alibaba.jstorm.schedule;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.generated.StormTopology;

import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;

public class TopologyAssignContext {

	public static final int ASSIGN_TYPE_NEW = 0; // assign a new topology

	public static final int ASSIGN_TYPE_REBALANCE = 1; // rebalance a topology

	public static final int ASSIGN_TYPE_MONITOR = 2; // monitor a topology, some
														// tasks are dead

	protected int assignType;

	protected StormTopology rawTopology;

	protected Map stormConf;

	// if assignType is ASSIGN_TYPE_NEW, oldAssignment is the Assignment last
	// time
	// otherwise it is the old assignment before assignment.
	protected Assignment oldAssignment;

	protected Map<String, SupervisorInfo> cluster;

	protected Map<Integer, String> taskToComponent;

	protected Set<Integer> allTaskIds; // all tasks
	protected Set<Integer> deadTaskIds; // dead tasks
	protected Set<Integer> unstoppedTaskIds; // the task is alive, but his
												// supervisor is dead
	protected Set<ResourceWorkerSlot> unstoppedWorkers;

	public TopologyAssignContext() {

	}

	public TopologyAssignContext(TopologyAssignContext copy) {
		this.assignType = copy.getAssignType();
		this.rawTopology = copy.getRawTopology();
		this.stormConf = copy.getStormConf();
		this.oldAssignment = copy.getOldAssignment();
		this.cluster = copy.getCluster();
		this.taskToComponent = copy.getTaskToComponent();
		this.allTaskIds = copy.getAllTaskIds();
		this.deadTaskIds = copy.getDeadTaskIds();
		this.unstoppedTaskIds = copy.getUnstoppedTaskIds();
	}

	public int getAssignType() {
		return assignType;
	}

	public void setAssignType(int assignType) {
		this.assignType = assignType;
	}

	public StormTopology getRawTopology() {
		return rawTopology;
	}

	public void setRawTopology(StormTopology rawTopology) {
		this.rawTopology = rawTopology;
	}

	public Map getStormConf() {
		return stormConf;
	}

	public void setStormConf(Map stormConf) {
		this.stormConf = stormConf;
	}

	public Assignment getOldAssignment() {
		return oldAssignment;
	}

	public void setOldAssignment(Assignment oldAssignment) {
		this.oldAssignment = oldAssignment;
	}

	public Map<String, SupervisorInfo> getCluster() {
		return cluster;
	}

	public void setCluster(Map<String, SupervisorInfo> cluster) {
		this.cluster = cluster;
	}

	public Set<Integer> getAllTaskIds() {
		return allTaskIds;
	}

	public void setAllTaskIds(Set<Integer> allTaskIds) {
		this.allTaskIds = allTaskIds;
	}

	public Set<Integer> getDeadTaskIds() {
		return deadTaskIds;
	}

	public void setDeadTaskIds(Set<Integer> deadTaskIds) {
		this.deadTaskIds = deadTaskIds;
	}

	public Set<Integer> getUnstoppedTaskIds() {
		return unstoppedTaskIds;
	}

	public void setUnstoppedTaskIds(Set<Integer> unstoppedTaskIds) {
		this.unstoppedTaskIds = unstoppedTaskIds;
	}

	public Map<Integer, String> getTaskToComponent() {
		return taskToComponent;
	}

	public void setTaskToComponent(Map<Integer, String> taskToComponent) {
		this.taskToComponent = taskToComponent;
	}

	public static boolean isAssignTypeValid(int type) {
		return (type == ASSIGN_TYPE_NEW) || (type == ASSIGN_TYPE_REBALANCE)
				|| (type == ASSIGN_TYPE_MONITOR);
	}

	public Set<ResourceWorkerSlot> getUnstoppedWorkers() {
		return unstoppedWorkers;
	}

	public void setUnstoppedWorkers(Set<ResourceWorkerSlot> unstoppedWorkers) {
		this.unstoppedWorkers = unstoppedWorkers;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
