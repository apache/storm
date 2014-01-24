package com.alibaba.jstorm.schedule.default_assign;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.google.common.collect.Maps;

/**
 * Assign task to resource algorithm
 * 
 * @author zhongyan.feng
 * @version $Id:
 */
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
		Map<Integer, ResourceAssignment> oldAssigns = context
				.getOldAssignment().getTaskToResource();
		for (Integer task : canFree) {
			ResourceAssignment oldAssign = oldAssigns.get(task);
			if (oldAssign == null) {
				LOG.warn("When free rebalance resource, no ResourceAssignment of task "
						+ task);
				continue;
			}

			SupervisorInfo supervisorInfo = cluster.get(oldAssign
					.getSupervisorId());
			if (supervisorInfo == null) {
				continue;
			}
			supervisorInfo.getCpuPool()
					.free(oldAssign.getCpuSlotNum(), context);
			supervisorInfo.getMemPool()
					.free(oldAssign.getMemSlotNum(), context);
			supervisorInfo.getDiskPool().free(oldAssign.getDiskSlot(), context);
			supervisorInfo.getNetPool().free(oldAssign.getPort(), context);
		}
	}

	private int computeWeight(Map componentMap,
			DefaultTopologyAssignContext context) {
		int weight = 0;

		int cpu = ConfigExtension.getCpuSlotsPerTask(componentMap);
		int mem = ConfigExtension.getMemSlotPerTask(componentMap);
		weight = cpu * context.CPU_WEIGHT + mem * context.MEM_WEIGHT;

		if (ConfigExtension.isTaskAllocDisk(componentMap)) {
			// alloc disk
			weight += context.DISK_WEIGHT;
		} else if (ConfigExtension.isTaskOnDifferentNode(componentMap)) {
			// task run on different node
			weight += context.TASK_ON_DIFFERENT_NODE_WEIGHT;
		}

		return weight;
	}

	private Map<String, Integer> computeComponentWeight(
			StormTopology rawTopology, DefaultTopologyAssignContext context) {
		Map<String, Integer> ret = new HashMap<String, Integer>();

		Map<String, Object> components = ThriftTopologyUtils
				.getComponents(rawTopology);
		for (Entry<String, Object> entry : components.entrySet()) {
			String componentName = entry.getKey();
			Object component = entry.getValue();

			ComponentCommon common = null;
			if (component instanceof Bolt) {
				common = ((Bolt) component).get_common();
			}
			if (component instanceof SpoutSpec) {
				common = ((SpoutSpec) component).get_common();
			}
			if (component instanceof StateSpoutSpec) {
				common = ((StateSpoutSpec) component).get_common();
			}

			String jsonConfString = common.get_json_conf();
			if (jsonConfString == null) {
				ret.put(componentName, context.DEFAULT_WEIGHT);
				continue;
			}

			Map componentMap = new HashMap();
			componentMap.putAll((Map) JStormUtils.from_json(jsonConfString));

			int weight = computeWeight(componentMap, context);

			ret.put(componentName, weight);
		}
		return ret;
	}

	/**
	 * Sort the task list, higher priorty task will be assigned first.
	 * 
	 * 1. get the task list which need to be assigned 2. sort the task list
	 * according to Configuration Weight
	 * 
	 * @param context
	 * @return
	 */
	protected List<Integer> sortAssignTasks(
			final DefaultTopologyAssignContext context, Set<Integer> needAssign) {
		List<Integer> ret = new ArrayList<Integer>();

		StormTopology rawTopology = context.getRawTopology();
		final Map<Integer, String> taskToComponent = context
				.getTaskToComponent();

		final Map<String, Integer> componentToWeight = computeComponentWeight(
				rawTopology, context);

		ret = JStormUtils.mk_list(needAssign);
		Collections.sort(ret, new Comparator<Integer>() {

			private int getWeight(int taskId) {
				String component = taskToComponent.get(taskId);
				if (component == null) {
					// this shouldn't occur
					return context.DEFAULT_WEIGHT;
				}

				Integer weight = componentToWeight.get(component);
				if (weight == null) {
					return context.DEFAULT_WEIGHT;
				} else {
					return weight;
				}
			}

			@Override
			public int compare(Integer first, Integer second) {
				int firstWeight = getWeight(first);
				int secondWeight = getWeight(second);

				if (firstWeight != secondWeight) {
					return (secondWeight - firstWeight);
				} else {
					return (second - first);
				}
			}

		});

		return ret;
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

	private ComponentAssignType getComponentType(
			DefaultTopologyAssignContext defaultContext, Integer taskId) {
		StormTopology sysTopology = defaultContext.getSysTopology();
		final Map<Integer, String> taskToComponent = defaultContext
				.getTaskToComponent();

		String componentName = taskToComponent.get(taskId);
		if (componentName == null) {
			LOG.warn("No component name of " + taskId);
			return ComponentAssignType.NORMAL;
		}

		ComponentCommon common = ThriftTopologyUtils.getComponentCommon(
				sysTopology, componentName);

		String jsonConfString = common.get_json_conf();
		if (jsonConfString == null) {
			return ComponentAssignType.NORMAL;
		}

		Map componentMap = new HashMap();
		componentMap.putAll((Map) JStormUtils.from_json(jsonConfString));

		if (JStormServerConfig.getUserDefineAssignmentFromJson(componentMap) != null) {
			// user define assignment
			return ComponentAssignType.USER_DEFINE;
		} else if (ConfigExtension.isUseOldAssignment(componentMap)) {
			// use old assignment
			return ComponentAssignType.USE_OLD;
		} else if (ConfigExtension.isUseOldAssignment(defaultContext
				.getStormConf())) {
			// use old assignment
			return ComponentAssignType.USE_OLD;
		} else {
			return ComponentAssignType.NORMAL;
		}

	}

	private IPreassignTask getPreassignHandler(ComponentAssignType type) {
		if (type == ComponentAssignType.USER_DEFINE) {
			return new UserDefinePreAssign();
		} else if (type == ComponentAssignType.USE_OLD) {
			return new UseOldPreAssign();
		} else {
			return new NormalPreAssign(nimbusConf);
		}
	}

	private Map<ComponentAssignType, Pair<Set<Integer>, IPreassignTask>> registerPreAssignHandler(
			final DefaultTopologyAssignContext defaultContext,
			Set<Integer> needAssign) {

		Map<ComponentAssignType, Pair<Set<Integer>, IPreassignTask>> ret = new TreeMap<ComponentAssignType, Pair<Set<Integer>, IPreassignTask>>();
		for (Integer taskId : needAssign) {
			ComponentAssignType type = getComponentType(defaultContext, taskId);

			Pair<Set<Integer>, IPreassignTask> pair = ret.get(type);
			if (pair == null) {
				Set<Integer> tasks = new TreeSet<Integer>();
				IPreassignTask handler = getPreassignHandler(type);

				pair = new Pair<Set<Integer>, IPreassignTask>(tasks, handler);

				ret.put(type, pair);
			}

			Set<Integer> tasks = pair.getFirst();
			tasks.add(taskId);

		}

		if (ret.size() < ComponentAssignType.values().length) {
			for (ComponentAssignType type : ComponentAssignType.values()) {
				if (ret.containsKey(type) == false) {
					Set<Integer> tasks = new TreeSet<Integer>();
					IPreassignTask handler = getPreassignHandler(type);

					Pair pair = new Pair<Set<Integer>, IPreassignTask>(tasks,
							handler);

					ret.put(type, pair);
				}
			}
		}

		return ret;
	}

	/**
	 * Get the task Map which the task is alive and will be kept Only when type
	 * is ASSIGN_TYPE_MONITOR, it is valid
	 * 
	 * @param defaultContext
	 * @param needAssigns
	 * @return
	 */
	public Map<Integer, ResourceAssignment> getKeepAssign(
			DefaultTopologyAssignContext defaultContext,
			Set<Integer> needAssigns) {

		Set<Integer> keepAssignIds = new HashSet<Integer>();
		keepAssignIds.addAll(defaultContext.getAllTaskIds());
		keepAssignIds.removeAll(defaultContext.getUnstoppedTaskIds());
		keepAssignIds.removeAll(needAssigns);

		Map<Integer, ResourceAssignment> keeps = new HashMap<Integer, ResourceAssignment>();
		if (keepAssignIds.isEmpty()) {
			return keeps;
		}

		Assignment oldAssignment = defaultContext.getOldAssignment();
		if (oldAssignment == null) {
			return keeps;
		}
		Map<Integer, ResourceAssignment> olds = oldAssignment
				.getTaskToResource();

		for (Integer task : keepAssignIds) {
			ResourceAssignment oldResource = olds.get(task);
			if (oldResource == null) {
				LOG.warn("No old assignment of " + task + ", "
						+ defaultContext.toDetailString());
				continue;
			}

			keeps.put(task, oldResource);
		}

		return keeps;
	}

	public Set<String> getCanUsedSupervisors(
			DefaultTopologyAssignContext defaultContext,
			Set<String> usedSupervisorIds, int allocWorkerNum) {

		Set<String> allSupervisorIds = defaultContext.getCluster().keySet();
		Map stormConf = defaultContext.getStormConf();

		boolean usedSingleNode = ConfigExtension.isUseSingleNode(stormConf);

		if (usedSingleNode == true) {
			if (usedSupervisorIds.isEmpty()) {
				return allSupervisorIds;
			} else {
				return usedSupervisorIds;
			}
		} else {
			// if you hava 15 tasks but only 10 workers, when you assign 11th
			// task,
			// it only can be assigned on usedSupervisors because there is no
			// enough
			// workers.
			if (allocWorkerNum > usedSupervisorIds.size()) {
				return allSupervisorIds;
			} else {
				return usedSupervisorIds;
			}
		}

	}

	@Override
	public Map<Integer, ResourceAssignment> assignTasks(
			TopologyAssignContext context) throws FailedAssignTopologyException {

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

		Map<Integer, ResourceAssignment> keepAssigns = getKeepAssign(
				defaultContext, needAssignTasks);

		// please use tree map to make task sequence
		Map<Integer, ResourceAssignment> ret = new TreeMap<Integer, ResourceAssignment>();
		ret.putAll(keepAssigns);
		ret.putAll(defaultContext.getUnstoppedAssignments());

		Map<WorkerSlot, List<Integer>> keepAssignWorkers = Assignment
				.getWorkerTasks(keepAssigns);

		int allocWorkerNum = defaultContext.getTotalWorkerNum()
				- defaultContext.getUnstoppedWorkerNum()
				- keepAssignWorkers.size();
		if (allocWorkerNum <= 0) {
			LOG.warn("Don't need assign workers, all workers are fine "
					+ defaultContext.toDetailString());
			throw new FailedAssignTopologyException(
					"Don't need assign worker, all workers are fine ");
		}

		Set<String> outputConfigComponents = new HashSet<String>();

		Map<ComponentAssignType, Pair<Set<Integer>, IPreassignTask>> typeHandler = registerPreAssignHandler(
				defaultContext, needAssignTasks);

		Map<Integer, ResourceAssignment> newAssigns = new HashMap<Integer, ResourceAssignment>();
		Set<String> usedSupervisorIds = new HashSet<String>();
		List<Integer> lastFailed = new ArrayList<Integer>();

		for (Entry<ComponentAssignType, Pair<Set<Integer>, IPreassignTask>> entry : typeHandler
				.entrySet()) {
			ComponentAssignType type = entry.getKey();

			Set<Integer> tasks = entry.getValue().getFirst();
			IPreassignTask handler = entry.getValue().getSecond();

			tasks.addAll(lastFailed);
			lastFailed.clear();

			List<Integer> sortedTasks = sortAssignTasks(defaultContext, tasks);

			StormTopology sysTopology = defaultContext.getSysTopology();

			for (Integer task : sortedTasks) {
				Set<String> canUsedSupervisorIds = getCanUsedSupervisors(
						defaultContext, usedSupervisorIds, allocWorkerNum);

				String componentName = defaultContext.getTaskToComponent().get(
						task);
				ComponentCommon componentCommon = ThriftTopologyUtils
						.getComponentCommon(sysTopology, componentName);

				Map componentMap = (Map) JStormUtils.from_json(componentCommon
						.get_json_conf());
				if (componentMap == null) {
					componentMap = Maps.newHashMap();
				}

				if (outputConfigComponents.contains(componentName) == false) {
					LOG.info("Component map of " + componentName + "\n"
							+ componentMap);
					outputConfigComponents.add(componentName);
				}

				ResourceAssignment preAssignment = handler.preAssign(task,
						defaultContext, componentMap, componentName,
						canUsedSupervisorIds, ret, newAssigns);
				if (preAssignment == null) {
					// pre assign fail
					lastFailed.add(task);
				} else {
					// sucess to do preAssign
					SupervisorInfo supervisorInfo = defaultContext.getCluster()
							.get(preAssignment.getSupervisorId());
					LOG.info("Task " + task + " had been assigned to "
							+ supervisorInfo.getHostName());
					newAssigns.put(task, preAssignment);
					ret.put(task, preAssignment);
					usedSupervisorIds.add(preAssignment.getSupervisorId());
				}
			}
		}

		if (lastFailed.isEmpty() == false) {
			throw new FailedAssignTopologyException("Failed to assign tasks "
					+ lastFailed);
		}

		// Here just hardcode
		IPostAssignTask postAssignHandler = new PostAssignTaskPort();
		postAssignHandler
				.postAssign(defaultContext, newAssigns, allocWorkerNum);

		LOG.info("Keep Alive slots:" + keepAssigns);
		LOG.info("Unstopped slots:" + defaultContext.getUnstoppedAssignments());
		LOG.info("New assign slots:" + newAssigns);

		return ret;
	}

}
