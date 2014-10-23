package com.alibaba.jstorm.schedule.default_assign;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.schedule.default_assign.Selector.ComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.InputComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.Selector;
import com.alibaba.jstorm.schedule.default_assign.Selector.TotalTaskNumSelector;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public class TaskGanker {

	public static Logger LOG = Logger.getLogger(TaskGanker.class);

	public static final String ACKER_NAME = "__acker";

	private final TaskGankerContext taskContext;

	private List<ResourceWorkerSlot> assignments = new ArrayList<ResourceWorkerSlot>();

	private int workerNum;

	private int baseNum;

	private int otherNum;

	private Set<Integer> tasks;

	private DefaultTopologyAssignContext context;

	private Selector componentSelector;

	private Selector inputComponentSelector;

	private Selector totalTaskNumSelector;

	public TaskGanker(DefaultTopologyAssignContext context, Set<Integer> tasks,
			List<ResourceWorkerSlot> workers) {
		this.tasks = tasks;
		this.context = context;
		context.getRawTopology().get_spouts().keySet();
		this.taskContext = new TaskGankerContext(
				this.buildSupervisorToWorker(workers),
				Common.buildSpoutOutoputAndBoltInputMap(context));
		this.componentSelector = new ComponentNumSelector(taskContext);
		this.inputComponentSelector = new InputComponentNumSelector(taskContext);
		this.totalTaskNumSelector = new TotalTaskNumSelector(taskContext);
		if (tasks.size() == 0)
			return;
		this.setTaskNum();
	}

	public List<ResourceWorkerSlot> gankTask() {
		if (tasks.size() == 0)
			return assignments;
		Set<Integer> hadGanked = this.gankOnDifferentNodeTask();
		tasks.removeAll(hadGanked);
		Set<Integer> ackers = new HashSet<Integer>();
		for (Integer task : tasks) {
			String name = context.getTaskToComponent().get(task);
			if (name.equals(TaskGanker.ACKER_NAME)) {
				ackers.add(task);
				continue;
			}
			this.beginGank(name, task);
		}
		for (Integer task : ackers) {
			this.beginGank(TaskGanker.ACKER_NAME, task);
		}
		return assignments;
	}

	private void beginGank(String name, Integer task) {
		ResourceWorkerSlot worker = this.chooseWorker(name,
				new ArrayList<ResourceWorkerSlot>(taskContext
						.getWorkerToTaskNum().keySet()));
		this.pushTaskToWorker(task, name, worker);
	}

	private Set<Integer> gankOnDifferentNodeTask() {
		Set<Integer> result = new HashSet<Integer>();
		for (Integer task : tasks) {
			Map conf = Common.getComponentMap(context, task);
			if (ConfigExtension.isTaskOnDifferentNode(conf))
				result.add(task);
		}
		for (Integer task : result) {
			String name = context.getTaskToComponent().get(task);
			ResourceWorkerSlot worker = this.chooseWorker(name,
					this.getDifferentNodeTaskWokres(name));
			this.pushTaskToWorker(task, name, worker);
		}
		return result;
	}

	private Map<String, List<ResourceWorkerSlot>> buildSupervisorToWorker(
			List<ResourceWorkerSlot> workers) {
		Map<String, List<ResourceWorkerSlot>> supervisorToWorker = new HashMap<String, List<ResourceWorkerSlot>>();
		for (ResourceWorkerSlot worker : workers) {
			if (worker.getTasks() == null || worker.getTasks().size() == 0) {
				List<ResourceWorkerSlot> supervisor = supervisorToWorker
						.get(worker.getNodeId());
				if (supervisor == null) {
					supervisor = new ArrayList<ResourceWorkerSlot>();
					supervisorToWorker.put(worker.getNodeId(), supervisor);
				}
				supervisor.add(worker);
			} else {
				assignments.add(worker);
			}
		}
		this.workerNum = workers.size() - assignments.size();
		return supervisorToWorker;
	}

	private ResourceWorkerSlot chooseWorker(String name,
			List<ResourceWorkerSlot> workers) {
		List<ResourceWorkerSlot> result = this.componentSelector.select(
				workers, name);
		result = this.totalTaskNumSelector.select(result, name);
		if (name.equals(TaskGanker.ACKER_NAME))
			return result.iterator().next();
		return this.inputComponentSelector.select(result, name).iterator()
				.next();
	}

	private void pushTaskToWorker(Integer task, String name,
			ResourceWorkerSlot worker) {
		Set<Integer> tasks = worker.getTasks();
		if (tasks == null) {
			tasks = new HashSet<Integer>();
			worker.setTasks(tasks);
		}
		tasks.add(task);
		int taskNum = taskContext.getWorkerToTaskNum().get(worker);
		taskContext.getWorkerToTaskNum().put(worker, ++taskNum);
		if (otherNum <= 0) {
			if (taskNum == baseNum) {
				taskContext.getWorkerToTaskNum().remove(worker);
				assignments.add(worker);
			}
		} else {
			if (taskNum == (baseNum + 1)) {
				taskContext.getWorkerToTaskNum().remove(worker);
				otherNum--;
				assignments.add(worker);
			}
			if (otherNum <= 0) {
				List<ResourceWorkerSlot> needDelete = new ArrayList<ResourceWorkerSlot>();
				for (Entry<ResourceWorkerSlot, Integer> entry : taskContext
						.getWorkerToTaskNum().entrySet()) {
					if (entry.getValue() == baseNum)
						needDelete.add(entry.getKey());
				}
				for (ResourceWorkerSlot workerToDelete : needDelete) {
					taskContext.getWorkerToTaskNum().remove(workerToDelete);
					assignments.add(workerToDelete);
				}
			}
		}
		Map<String, Integer> components = taskContext.getWorkerToComponentNum()
				.get(worker);
		if (components == null) {
			components = new HashMap<String, Integer>();
			taskContext.getWorkerToComponentNum().put(worker, components);
		}
		Integer componentNum = components.get(name);
		if (componentNum == null) {
			componentNum = 0;
		}
		components.put(name, ++componentNum);
	}

	private void setTaskNum() {
		this.baseNum = tasks.size() / workerNum;
		this.otherNum = tasks.size() % workerNum;
		for (Entry<String, List<ResourceWorkerSlot>> entry : taskContext
				.getSupervisorToWorker().entrySet()) {
			for (ResourceWorkerSlot worker : entry.getValue()) {
				taskContext.getWorkerToTaskNum().put(worker, 0);
			}
		}
	}

	private List<ResourceWorkerSlot> getDifferentNodeTaskWokres(String name) {
		List<ResourceWorkerSlot> workers = new ArrayList<ResourceWorkerSlot>();
		workers.addAll(taskContext.getWorkerToTaskNum().keySet());
		for (Entry<String, List<ResourceWorkerSlot>> entry : taskContext
				.getSupervisorToWorker().entrySet()) {
			if (taskContext.getComponentNumOnSupervisor(entry.getKey(), name) != 0)
				workers.removeAll(entry.getValue());
		}
		if (workers.size() == 0)
			throw new FailedAssignTopologyException(
					"there's no enough supervisor for making component: "
							+ name + " 's tasks on different node");
		return workers;
	}
}
