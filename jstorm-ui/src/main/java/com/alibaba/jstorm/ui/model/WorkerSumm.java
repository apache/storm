package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import com.alibaba.jstorm.common.stats.StatBuckets;

import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.WorkerSummary;

/**
 * mainpage:SupervisorSummary
 * 
 * @author longda
 * 
 */
public class WorkerSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247856L;
	private String port;
	private String topology;
	private String uptime;
	private String tasks;
	private String components;
	private String cpuNum;
	private String memNum;
	private String disks;

	public WorkerSumm() {
	}

	public WorkerSumm(WorkerSummary workerSummary) {
		this.port = String.valueOf(workerSummary.get_port());
		this.topology = workerSummary.get_topology();

		StringBuilder taskSB = new StringBuilder();
		StringBuilder componentSB = new StringBuilder();
		StringBuilder diskSB = new StringBuilder();
		boolean isFirst = true;

		int cpuNum = 0;
		int memNum = 0;

		int minUptime = 0;
		for (TaskSummary taskSummary : workerSummary.get_tasks()) {
			if (isFirst == false) {
				taskSB.append(',');
				componentSB.append(',');
			} else {
				minUptime = taskSummary.get_uptime_secs();
			}

			taskSB.append(taskSummary.get_task_id());
			componentSB.append(taskSummary.get_component_id());

			String disk = taskSummary.get_disk();
			if (disk != null && disk.isEmpty() != false) {
				diskSB.append(disk + " ");
			}
			cpuNum += taskSummary.get_cpu();
			memNum += taskSummary.get_mem();

			if (minUptime < taskSummary.get_uptime_secs()) {
				minUptime = taskSummary.get_uptime_secs();
			}

			isFirst = false;
		}

		this.uptime = StatBuckets.prettyUptimeStr(minUptime);
		this.tasks = taskSB.toString();
		this.components = componentSB.toString();
		this.cpuNum = String.valueOf(cpuNum);
		this.memNum = String.valueOf(memNum);
		this.disks = diskSB.toString();
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getTopology() {
		return topology;
	}

	public void setTopology(String topology) {
		this.topology = topology;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}

	public String getTasks() {
		return tasks;
	}

	public void setTasks(String tasks) {
		this.tasks = tasks;
	}

	public String getComponents() {
		return components;
	}

	public void setComponents(String components) {
		this.components = components;
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

	public String getDisks() {
		return disks;
	}

	public void setDisks(String disks) {
		this.disks = disks;
	}

}
