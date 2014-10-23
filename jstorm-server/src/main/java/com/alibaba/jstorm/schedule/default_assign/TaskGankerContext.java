package com.alibaba.jstorm.schedule.default_assign;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskGankerContext {

	private final Map<String, List<ResourceWorkerSlot>> supervisorToWorker;

	private final Map<String, Set<String>> relationship;

	private final Map<ResourceWorkerSlot, Map<String, Integer>> workerToComponentNum = new HashMap<ResourceWorkerSlot, Map<String, Integer>>();

	public Map<ResourceWorkerSlot, Integer> getWorkerToTaskNum() {
		return workerToTaskNum;
	}

	private final Map<ResourceWorkerSlot, Integer> workerToTaskNum = new HashMap<ResourceWorkerSlot, Integer>();

	public TaskGankerContext(
			Map<String, List<ResourceWorkerSlot>> supervisorToWorker,
			Map<String, Set<String>> relationship) {
		this.supervisorToWorker = supervisorToWorker;
		this.relationship = relationship;
	}

	public Map<String, List<ResourceWorkerSlot>> getSupervisorToWorker() {
		return supervisorToWorker;
	}

	public Map<ResourceWorkerSlot, Map<String, Integer>> getWorkerToComponentNum() {
		return workerToComponentNum;
	}

	public Map<String, Set<String>> getRelationship() {
		return relationship;
	}

	public int getComponentNumOnSupervisor(String supervisor, String name) {
		List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
		if (workers == null)
			return 0;
		int result = 0;
		for (ResourceWorkerSlot worker : workers) {
			result = result + this.getComponentNumOnWorker(worker, name);
		}
		return result;
	}

	public int getComponentNumOnWorker(ResourceWorkerSlot worker, String name) {
		int result = 0;
		Map<String, Integer> componentNum = workerToComponentNum.get(worker);
		if (componentNum != null && componentNum.get(name) != null)
			result = componentNum.get(name);
		return result;
	}

	public int getTaskNumOnSupervisor(String supervisor) {
		List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
		if (workers == null)
			return 0;
		int result = 0;
		for (ResourceWorkerSlot worker : workers) {
			result = result + this.getTaskNumOnWorker(worker);
		}
		return result;
	}

	public int getTaskNumOnWorker(ResourceWorkerSlot worker) {
		return worker.getTasks() == null ? 0 : worker.getTasks().size();
	}

	public int getInputComponentNumOnSupervisor(String supervisor, String name) {
		int result = 0;
		List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
		if (workers == null)
			return 0;
		for (ResourceWorkerSlot worker : workers)
			result = result + this.getInputComponentNumOnWorker(worker, name);
		return result;
	}

	public int getInputComponentNumOnWorker(ResourceWorkerSlot worker,
			String name) {
		int result = 0;
		for (String component : relationship.get(name))
			result = result + this.getComponentNumOnWorker(worker, component);
		return result;
	}

}
