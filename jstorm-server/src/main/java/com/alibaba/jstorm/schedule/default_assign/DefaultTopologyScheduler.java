package com.alibaba.jstorm.schedule.default_assign;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public class DefaultTopologyScheduler implements IToplogyScheduler {
	private static final Logger LOG = Logger
			.getLogger(DefaultTopologyScheduler.class);

	private Map nimbusConf;

	@Override
	public void prepare(Map conf) {
		nimbusConf = conf;
	}

	/**
	 * @@@ Here maybe exist one problem, some dead slots have been free
	 * 
	 * @param context
	 */
	protected void freeUsed(TopologyAssignContext context) {
		Set<Integer> canFree = new HashSet<Integer>();
		canFree.addAll(context.getAllTaskIds());
		canFree.removeAll(context.getUnstoppedTaskIds());

		Map<String, SupervisorInfo> cluster = context.getCluster();
		Assignment oldAssigns = context.getOldAssignment();
		for (Integer task : canFree) {
			ResourceWorkerSlot worker = oldAssigns.getWorkerByTaskId(task);
			if (worker == null) {
				LOG.warn("When free rebalance resource, no ResourceAssignment of task "
						+ task);
				continue;
			}

			SupervisorInfo supervisorInfo = cluster.get(worker.getNodeId());
			if (supervisorInfo == null) {
				continue;
			}
			supervisorInfo.getWorkerPorts().add(worker.getPort());
		}
	}

	private Set<Integer> getNeedAssignTasks(DefaultTopologyAssignContext context) {
		Set<Integer> needAssign = new HashSet<Integer>();

		int assignType = context.getAssignType();
		if (assignType == TopologyAssignContext.ASSIGN_TYPE_NEW) {
			needAssign.addAll(context.getAllTaskIds());
		} else if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
			needAssign.addAll(context.getAllTaskIds());
			needAssign.removeAll(context.getUnstoppedTaskIds());
		} else {
			// monitor
			needAssign.addAll(context.getDeadTaskIds());
		}

		return needAssign;
	}

	/**
	 * Get the task Map which the task is alive and will be kept Only when type
	 * is ASSIGN_TYPE_MONITOR, it is valid
	 * 
	 * @param defaultContext
	 * @param needAssigns
	 * @return
	 */
	public Set<ResourceWorkerSlot> getKeepAssign(
			DefaultTopologyAssignContext defaultContext,
			Set<Integer> needAssigns) {

		Set<Integer> keepAssignIds = new HashSet<Integer>();
		keepAssignIds.addAll(defaultContext.getAllTaskIds());
		keepAssignIds.removeAll(defaultContext.getUnstoppedTaskIds());
		keepAssignIds.removeAll(needAssigns);
		Set<ResourceWorkerSlot> keeps = new HashSet<ResourceWorkerSlot>();
		if (keepAssignIds.isEmpty()) {
			return keeps;
		}

		Assignment oldAssignment = defaultContext.getOldAssignment();
		if (oldAssignment == null) {
			return keeps;
		}
		keeps.addAll(defaultContext.getOldWorkers());
		for (ResourceWorkerSlot worker : defaultContext.getOldWorkers()) {
			for (Integer task : worker.getTasks()) {
				if (!keepAssignIds.contains(task)) {
					keeps.remove(worker);
					break;
				}
			}
		}
		return keeps;
	}

	@Override
	public Set<ResourceWorkerSlot> assignTasks(TopologyAssignContext context)
			throws FailedAssignTopologyException {

		int assignType = context.getAssignType();
		if (TopologyAssignContext.isAssignTypeValid(assignType) == false) {
			throw new FailedAssignTopologyException("Invalide Assign Type "
					+ assignType);
		}

		DefaultTopologyAssignContext defaultContext = new DefaultTopologyAssignContext(
				context);
		if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
			freeUsed(defaultContext);
		}
		LOG.info("Dead tasks:" + defaultContext.getDeadTaskIds());
		LOG.info("Unstopped tasks:" + defaultContext.getUnstoppedTaskIds());

		Set<Integer> needAssignTasks = getNeedAssignTasks(defaultContext);

		Set<ResourceWorkerSlot> keepAssigns = getKeepAssign(defaultContext,
				needAssignTasks);

		// please use tree map to make task sequence
		Set<ResourceWorkerSlot> ret = new HashSet<ResourceWorkerSlot>();
		ret.addAll(keepAssigns);
		ret.addAll(defaultContext.getUnstoppedWorkers());

		int allocWorkerNum = defaultContext.getTotalWorkerNum()
				- defaultContext.getUnstoppedWorkerNum() - keepAssigns.size();

		if (allocWorkerNum <= 0) {
			LOG.warn("Don't need assign workers, all workers are fine "
					+ defaultContext.toDetailString());
			throw new FailedAssignTopologyException(
					"Don't need assign worker, all workers are fine ");
		}

		List<ResourceWorkerSlot> newAssignList = WorkerMaker.getInstance()
				.makeWorkers(defaultContext, needAssignTasks, allocWorkerNum);
		TaskGanker ganker = new TaskGanker(defaultContext, needAssignTasks,
				newAssignList);
		Set<ResourceWorkerSlot> newAssigns = new HashSet<ResourceWorkerSlot>(
				ganker.gankTask());
		ret.addAll(newAssigns);

		LOG.info("Keep Alive slots:" + keepAssigns);
		LOG.info("Unstopped slots:" + defaultContext.getUnstoppedWorkers());
		LOG.info("New assign slots:" + newAssigns);

		return ret;
	}

}
