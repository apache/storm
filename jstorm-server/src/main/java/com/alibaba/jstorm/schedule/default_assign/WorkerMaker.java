package com.alibaba.jstorm.schedule.default_assign;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class WorkerMaker {

	public static Logger LOG = Logger.getLogger(WorkerMaker.class);

	private static WorkerMaker instance;

	private WorkerMaker() {

	}

	public static WorkerMaker getInstance() {
		if (instance == null) {
			instance = new WorkerMaker();
		}
		return instance;
	}

	public List<ResourceWorkerSlot> makeWorkers(
			DefaultTopologyAssignContext context, Set<Integer> needAssign,
			int num) {
		int workersNum = getWorkersNum(context, num);
		if (workersNum == 0) {
			throw new FailedAssignTopologyException("there's no enough worker");
		}
		LOG.info("worker num is: " + workersNum);
		List<ResourceWorkerSlot> result = new ArrayList<ResourceWorkerSlot>();
		// userdefine assignments
		this.getRightWorkers(
				context,
				needAssign,
				result,
				workersNum,
				getUserDefineWorkers(context, ConfigExtension
						.getUserDefineAssignment(context.getStormConf())));
		// old assignments
		if (ConfigExtension.isUseOldAssignment(context.getStormConf())) {
			this.getRightWorkers(context, needAssign, result, workersNum,
					context.getOldWorkers());
		}
		int defaultWorkerNum = Math.min(workersNum - result.size(),
				needAssign.size());
		LOG.info("Get workers from user define and old assignments: " + result);
		LOG.info("Tasks: " + needAssign + " will be scheduled by default"
				+ " in " + defaultWorkerNum + " workers");
		for (int i = 0; i < defaultWorkerNum; i++) {
			result.add(new ResourceWorkerSlot());
		}
		List<SupervisorInfo> isolationSupervisors = this
				.getIsolationSupervisors(context);
		if (isolationSupervisors.size() != 0) {
			this.putAllWorkerToSupervisor(result,
					this.getCanUseSupervisors(isolationSupervisors));
		} else {
			this.putAllWorkerToSupervisor(result,
					this.getCanUseSupervisors(context.getCluster()));
		}
		this.setAllWorkerMemAndCpu(context.getStormConf(), result);
		return result;
	}

	private void setAllWorkerMemAndCpu(Map conf, List<ResourceWorkerSlot> result) {
		long defaultSize = ConfigExtension.getMemSizePerWorker(conf);
		int defaultCpu = ConfigExtension.getCpuSlotPerWorker(conf);
		for (ResourceWorkerSlot worker : result) {
			if (worker.getMemSize() <= 0)
				worker.setMemSize(defaultSize);
			if (worker.getCpu() <= 0)
				worker.setCpu(defaultCpu);
		}
	}

	private void putAllWorkerToSupervisor(List<ResourceWorkerSlot> result,
			List<SupervisorInfo> supervisors) {
		for (ResourceWorkerSlot worker : result) {
			if (worker.getNodeId() != null)
				continue;
			if (worker.getHostname() != null) {
				for (SupervisorInfo supervisor : supervisors) {
					if (NetWorkUtils.equals(supervisor.getHostName(), worker.getHostname())
							&& supervisor.getWorkerPorts().size() > 0) {
						this.putWorkerToSupervisor(supervisor, worker);
						break;
					}
				}
			}
		}
		supervisors = this.getCanUseSupervisors(supervisors);
		Collections.sort(supervisors, new Comparator<SupervisorInfo>() {

			@Override
			public int compare(SupervisorInfo o1, SupervisorInfo o2) {
				// TODO Auto-generated method stub
				return -NumberUtils.compare(o1.getWorkerPorts().size(), o2
						.getWorkerPorts().size());
			}

		});
		this.putWorkerToSupervisor(result, supervisors);
	}

	private void putWorkerToSupervisor(SupervisorInfo supervisor,
			ResourceWorkerSlot worker) {
		int port = worker.getPort();
		if (!supervisor.getWorkerPorts().contains(worker.getPort())) {
			port = supervisor.getWorkerPorts().iterator().next();
		}
		worker.setPort(port);
		supervisor.getWorkerPorts().remove(port);
		worker.setNodeId(supervisor.getSupervisorId());
	}

	private void putWorkerToSupervisor(List<ResourceWorkerSlot> result,
			List<SupervisorInfo> supervisors) {
		int key = 0;
		for (ResourceWorkerSlot worker : result) {
			if (supervisors.size() == 0)
				return;
			if (worker.getNodeId() != null)
				continue;
			if (key >= supervisors.size())
				key = 0;
			SupervisorInfo supervisor = supervisors.get(key);
			worker.setHostname(supervisor.getHostName());
			worker.setNodeId(supervisor.getSupervisorId());
			worker.setPort(supervisor.getWorkerPorts().iterator().next());
			supervisor.getWorkerPorts().remove(worker.getPort());
			if (supervisor.getWorkerPorts().size() == 0)
				supervisors.remove(supervisor);
			key++;
		}
	}

	private void getRightWorkers(DefaultTopologyAssignContext context,
			Set<Integer> needAssign, List<ResourceWorkerSlot> result,
			int workersNum, Collection<ResourceWorkerSlot> workers) {
		Set<Integer> assigned = new HashSet<Integer>();
		List<ResourceWorkerSlot> users = new ArrayList<ResourceWorkerSlot>();
		if (workers == null)
			return;
		for (ResourceWorkerSlot worker : workers) {
			boolean right = true;
			Set<Integer> tasks = worker.getTasks();
			if (tasks == null)
				continue;
			for (Integer task : tasks) {
				if (!needAssign.contains(task) || assigned.contains(task)) {
					right = false;
					break;
				}
			}
			if (right) {
				assigned.addAll(tasks);
				users.add(worker);
			}
		}
		if (users.size() + result.size() > workersNum) {
			return;
		}

		if (users.size() + result.size() == workersNum
				&& assigned.size() != needAssign.size()) {
			return;
		}
		result.addAll(users);
		needAssign.removeAll(assigned);
	}

	private int getWorkersNum(DefaultTopologyAssignContext context,
			int workersNum) {
		Map<String, SupervisorInfo> supervisors = context.getCluster();
		List<SupervisorInfo> isolationSupervisors = this
				.getIsolationSupervisors(context);
		int slotNum = 0;

		if (isolationSupervisors.size() != 0) {
			for (SupervisorInfo superivsor : isolationSupervisors) {
				slotNum = slotNum + superivsor.getWorkerPorts().size();
			}
			return Math.min(slotNum, workersNum);
		}
		for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
			slotNum = slotNum + entry.getValue().getWorkerPorts().size();
		}
		return Math.min(slotNum, workersNum);
	}

	/**
	 * @param context
	 * @param workers
	 * @return
	 */
	private List<ResourceWorkerSlot> getUserDefineWorkers(
			DefaultTopologyAssignContext context, List<WorkerAssignment> workers) {
		List<ResourceWorkerSlot> ret = new ArrayList<ResourceWorkerSlot>();
		if (workers == null)
			return ret;
		Map<String, List<Integer>> componentToTask = (HashMap<String, List<Integer>>) ((HashMap<String, List<Integer>>) context
				.getComponentTasks()).clone();
		if (context.getAssignType() != context.ASSIGN_TYPE_NEW) {
			this.checkUserDefineWorkers(context, workers,
					context.getTaskToComponent());
		}
		for (WorkerAssignment worker : workers) {
			ResourceWorkerSlot workerSlot = new ResourceWorkerSlot(worker,
					componentToTask);
			if (workerSlot.getTasks().size() != 0) {
				ret.add(workerSlot);
			}
		}
		return ret;
	}

	private void checkUserDefineWorkers(DefaultTopologyAssignContext context,
			List<WorkerAssignment> workers, Map<Integer, String> taskToComponent) {
		Set<ResourceWorkerSlot> unstoppedWorkers = context
				.getUnstoppedWorkers();
		List<WorkerAssignment> re = new ArrayList<WorkerAssignment>();
		for (WorkerAssignment worker : workers) {
			for (ResourceWorkerSlot unstopped : unstoppedWorkers) {
				if (unstopped
						.compareToUserDefineWorker(worker, taskToComponent))
					re.add(worker);
			}
		}
		workers.removeAll(re);

	}

	private List<SupervisorInfo> getCanUseSupervisors(
			Map<String, SupervisorInfo> supervisors) {
		List<SupervisorInfo> canUseSupervisors = new ArrayList<SupervisorInfo>();
		for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
			SupervisorInfo supervisor = entry.getValue();
			if (supervisor.getWorkerPorts().size() > 0)
				canUseSupervisors.add(entry.getValue());
		}
		return canUseSupervisors;
	}

	private List<SupervisorInfo> getCanUseSupervisors(
			List<SupervisorInfo> supervisors) {
		List<SupervisorInfo> canUseSupervisors = new ArrayList<SupervisorInfo>();
		for (SupervisorInfo supervisor : supervisors) {
			if (supervisor.getWorkerPorts().size() > 0)
				canUseSupervisors.add(supervisor);
		}
		return canUseSupervisors;
	}

	private List<SupervisorInfo> getIsolationSupervisors(
			DefaultTopologyAssignContext context) {
		List<String> isolationHosts = (List<String>) context.getStormConf()
				.get(Config.ISOLATION_SCHEDULER_MACHINES);
		LOG.info("Isolation machines: " + isolationHosts);
		if (isolationHosts == null)
			return new ArrayList<SupervisorInfo>();
		List<SupervisorInfo> isolationSupervisors = new ArrayList<SupervisorInfo>();
		for (Entry<String, SupervisorInfo> entry : context.getCluster()
				.entrySet()) {
			if (containTargetHost(isolationHosts, entry.getValue().getHostName())) {
				isolationSupervisors.add(entry.getValue());
			}
		}
		return isolationSupervisors;
	}
	
	private boolean containTargetHost(Collection<String> hosts, String target) {
		for (String host : hosts) {
			if (NetWorkUtils.equals(host, target) == true) {
				return true;
			}
		}
		return false;
	}
}
