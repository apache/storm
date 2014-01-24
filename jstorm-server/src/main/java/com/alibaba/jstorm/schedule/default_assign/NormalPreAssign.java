package com.alibaba.jstorm.schedule.default_assign;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.resource.ResourceType;
import com.alibaba.jstorm.resource.TaskAllocResource;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;

public class NormalPreAssign implements IPreassignTask {
	private static final Logger LOG = Logger.getLogger(NormalPreAssign.class);

	private Map nimbusConf;

	NormalPreAssign(Map nimbusConf) {
		this.nimbusConf = nimbusConf;
	}

	private Map<Integer, Set<ResourceType>> getWeightTree(
			DefaultTopologyAssignContext defaultContext) {
		Map<Integer, Set<ResourceType>> weight = new TreeMap<Integer, Set<ResourceType>>(
				new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {

						return o2.compareTo(o1);
					}

				});

		Set<ResourceType> cache = weight.get(defaultContext.CPU_WEIGHT);
		if (cache == null) {
			cache = new HashSet<ResourceType>();
			weight.put(defaultContext.CPU_WEIGHT, cache);
		}
		cache.add(ResourceType.CPU);

		cache = weight.get(defaultContext.MEM_WEIGHT);
		if (cache == null) {
			cache = new HashSet<ResourceType>();
			weight.put(defaultContext.MEM_WEIGHT, cache);
		}
		cache.add(ResourceType.MEM);

		cache = weight.get(defaultContext.DISK_WEIGHT);
		if (cache == null) {
			cache = new HashSet<ResourceType>();
			weight.put(defaultContext.DISK_WEIGHT, cache);
		}
		cache.add(ResourceType.DISK);

		cache = weight.get(defaultContext.PORT_WEIGHT);
		if (cache == null) {
			cache = new HashSet<ResourceType>();
			weight.put(defaultContext.PORT_WEIGHT, cache);
		}
		cache.add(ResourceType.NET);

		return weight;
	}

	private Integer getLeftSlot(SupervisorInfo supervisorInfo,
			ResourceType type, TaskAllocResource taskAllocResource,
			Integer taskNum) {
		if (type == ResourceType.CPU) {
			return (supervisorInfo.getCpuPool().getLeftNum() - taskAllocResource
					.getCpuSlotNum());
		} else if (type == ResourceType.MEM) {
			return (supervisorInfo.getMemPool().getLeftNum() - taskAllocResource
					.getMemSlotNum());
		} else if (type == ResourceType.DISK) {
			if (taskAllocResource.isAllocDisk() == false) {
				return 0;
			} else {
				return (supervisorInfo.getDiskPool().getLeftNum() - 1);
			}
		} else {
			return (supervisorInfo.getNetPool().getLeftNum() - taskNum);
		}
	}

	private Map<Integer, Map<String, Integer>> getWeightResourceMap(
			DefaultTopologyAssignContext defaultContext,
			TaskAllocResource taskAllocResource, Set<String> supervisors,
			Map<String, Integer> newSupervisorTaskNum) {

		Map<Integer, Set<ResourceType>> weightMap = getWeightTree(defaultContext);

		Map<Integer, Map<String, Integer>> ret = new TreeMap<Integer, Map<String, Integer>>(
				new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {

						return o2.compareTo(o1);
					}

				});

		for (Entry<Integer, Set<ResourceType>> entry : weightMap.entrySet()) {
			Integer weight = entry.getKey();
			Set<ResourceType> types = entry.getValue();

			for (ResourceType type : types) {
				Map<String, Integer> supervisorLeftSlots = ret.get(weight);
				if (supervisorLeftSlots == null) {
					supervisorLeftSlots = new TreeMap<String, Integer>();
					ret.put(weight, supervisorLeftSlots);
				}

				for (String sid : supervisors) {
					SupervisorInfo supervisorInfo = defaultContext.getCluster()
							.get(sid);
					Integer taskNum = newSupervisorTaskNum.get(sid);
					if (taskNum == null) {
						taskNum = Integer.valueOf(0);
					}
					Integer left = getLeftSlot(supervisorInfo, type,
							taskAllocResource, taskNum);

					Integer leftSum = supervisorLeftSlots.get(sid);
					if (leftSum == null) {
						leftSum = Integer.valueOf(0);
					}

					leftSum = leftSum + left;

					supervisorLeftSlots.put(sid, leftSum);
				}
			}
		}

		return ret;

	}

	private Set<String> selectMost(Map<String, Integer> slotNums,
			Set<String> supervisorIds) {
		Set<String> ret = null;
		Integer max = Integer.MIN_VALUE;

		for (String sid : supervisorIds) {
			Integer left = slotNums.get(sid);

			if (left > max) {
				max = left;
				ret = new HashSet<String>();
				ret.add(sid);

			} else if (left == max) {
				ret.add(sid);
			}
		}

		return ret;
	}

	private String selectOne(
			Map<Integer, Map<String, Integer>> weightResourceMap,
			Set<String> supervisorIds) {

		Set<String> leastSet = supervisorIds;
		for (Entry<Integer, Map<String, Integer>> entry : weightResourceMap
				.entrySet()) {
			if (leastSet.size() == 1) {
				break;
			}
			leastSet = selectMost(entry.getValue(), leastSet);
		}

		List<String> list = JStormUtils.mk_list(leastSet);
		return list.get(0);
	}

	/**
	 * 
	 * 
	 * @param defaultContext
	 * @param taskAllocResource
	 * @param supervisors
	 * @param newAssigns
	 * @return
	 */
	private String selectBestSupervisorByLevel(
			DefaultTopologyAssignContext defaultContext,
			TaskAllocResource taskAllocResource, Set<String> supervisors,
			Map<Integer, ResourceAssignment> newAssigns) {

		Map<String, Integer> newSupervisorTaskNum = new HashMap<String, Integer>();
		for (Entry<Integer, ResourceAssignment> entry : newAssigns.entrySet()) {
			ResourceAssignment assignment = entry.getValue();
			String supervisorId = assignment.getSupervisorId();

			Integer num = newSupervisorTaskNum.get(supervisorId);
			if (num == null) {
				num = Integer.valueOf(0);
			}

			num = num + 1;
			newSupervisorTaskNum.put(supervisorId, num);

		}

		Map<Integer, Map<String, Integer>> weightResourceMap = getWeightResourceMap(
				defaultContext, taskAllocResource, supervisors,
				newSupervisorTaskNum);

		return selectOne(weightResourceMap, supervisors);

	}

	private int computeSupervisorWeight(
			DefaultTopologyAssignContext defaultContext,
			SupervisorInfo supervisorInfo, TaskAllocResource taskAllocResource,
			Integer allocTaskNum) {
		int diskWeight = 0;
		if (taskAllocResource.isAllocDisk()) {
			// alloc disk
			diskWeight = (supervisorInfo.getDiskPool().getLeftNum() - 1)
					* defaultContext.DISK_WEIGHT;
		}

		int leftCpu = supervisorInfo.getCpuPool().getLeftNum();
		int leftMem = supervisorInfo.getMemPool().getLeftNum();
		int portNum = supervisorInfo.getNetPool().getLeftNum();

		int ret = diskWeight;
		ret += (leftCpu - taskAllocResource.getCpuSlotNum())
				* defaultContext.CPU_WEIGHT;
		ret += (leftMem - taskAllocResource.getMemSlotNum())
				* defaultContext.MEM_WEIGHT;
		ret += (portNum - allocTaskNum) * defaultContext.PORT_WEIGHT;

		return ret;
	}

	private String selectBestSupervisorByWeight(
			DefaultTopologyAssignContext defaultContext,
			TaskAllocResource taskAllocResource, Set<String> supervisors,
			Map<Integer, ResourceAssignment> newAssigns) {

		String bestSupervisor = null;
		int maxWeight = Integer.MIN_VALUE;

		Map<String, Integer> newSupervisorTaskNum = new HashMap<String, Integer>();
		for (Entry<Integer, ResourceAssignment> entry : newAssigns.entrySet()) {
			ResourceAssignment assignment = entry.getValue();
			String supervisorId = assignment.getSupervisorId();

			Integer num = newSupervisorTaskNum.get(supervisorId);
			if (num == null) {
				num = Integer.valueOf(0);
			}

			num = num + 1;
			newSupervisorTaskNum.put(supervisorId, num);

		}

		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();
		for (String supervisorId : supervisors) {
			SupervisorInfo supervisorInfo = cluster.get(supervisorId);
			Integer allocTaskNum = newSupervisorTaskNum.get(supervisorId);
			if (allocTaskNum == null) {
				allocTaskNum = Integer.valueOf(0);
			}

			int weight = computeSupervisorWeight(defaultContext,
					supervisorInfo, taskAllocResource, allocTaskNum);
			if (weight > maxWeight) {
				maxWeight = weight;
				bestSupervisor = supervisorId;
			}
		}

		return bestSupervisor;

	}

	private boolean isSelectByLevel(Map topologyConf) {
		Map tmpConf = new HashMap();
		tmpConf.putAll(nimbusConf);
		tmpConf.putAll(topologyConf);

		return ConfigExtension.isTopologyAssignSupervisorBylevel(tmpConf);
	}

	private String selectBestSupervisor(
			DefaultTopologyAssignContext defaultContext,
			TaskAllocResource taskAllocResource, Set<String> supervisors,
			Map<Integer, ResourceAssignment> newAssigns) {

		boolean isByLevel = isSelectByLevel(defaultContext.getStormConf());

		if (isByLevel == false) {
			return selectBestSupervisorByWeight(defaultContext,
					taskAllocResource, supervisors, newAssigns);
		} else {
			return selectBestSupervisorByLevel(defaultContext,
					taskAllocResource, supervisors, newAssigns);
		}

	}

	@Override
	public ResourceAssignment preAssign(Integer task,
			DefaultTopologyAssignContext defaultContext, Map componentMap,
			String componentName, Set<String> canUsedSupervisorIds,
			Map<Integer, ResourceAssignment> alreadyAssign,
			Map<Integer, ResourceAssignment> newAssigns) {

		Set<String> resEnoughSupervisorIds = new HashSet<String>();

		TaskAllocResource taskAlloc = AssignTaskUtils.getTaskAlloc(task,
				defaultContext, componentMap, componentName);
		StringBuilder failMessage = new StringBuilder();
		for (String supervisorId : canUsedSupervisorIds) {
			SupervisorInfo supervisorInfo = defaultContext.getCluster().get(
					supervisorId);
			String lackingResource = AssignTaskUtils.isSupervisorAvailable(
					defaultContext, supervisorInfo, taskAlloc);
			if (lackingResource != null) {
				failMessage.append(supervisorInfo.getHostName());
				failMessage.append(" has not enough resource of ");
				failMessage.append(lackingResource);
				failMessage.append("\n");
				continue;
			}

			boolean isDifferent = AssignTaskUtils.isIsolate(task, supervisorId,
					componentName, componentMap, defaultContext, alreadyAssign);
			if (isDifferent == false) {
				failMessage.append(supervisorInfo.getHostName());
				failMessage.append(" TaskOnDifferent fail for ");
				failMessage.append(componentName);
				failMessage.append("\n");
				continue;
			}

			resEnoughSupervisorIds.add(supervisorId);
		}

		if (resEnoughSupervisorIds.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			sb.append("No supervisor resource is enough for component ");
			sb.append(componentName);
			sb.append(" " + taskAlloc + " ");
			sb.append("\n\n");
			sb.append("Cluster resource:");
			sb.append(defaultContext.getCluster());
			sb.append("\n\n");
			sb.append(failMessage.toString());
			throw new FailedAssignTopologyException(sb.toString());
		}

		String bestSupervisorId = selectBestSupervisor(defaultContext,
				taskAlloc, resEnoughSupervisorIds, newAssigns);

		return AssignTaskUtils.allocResource(defaultContext, taskAlloc,
				bestSupervisorId);
	}

}
