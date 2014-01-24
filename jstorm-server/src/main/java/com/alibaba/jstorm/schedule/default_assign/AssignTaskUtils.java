package com.alibaba.jstorm.schedule.default_assign;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.resource.TaskAllocResource;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;

public class AssignTaskUtils {
	private static final Logger LOG = Logger.getLogger(AssignTaskUtils.class);

	private static final String LACK_CPU = " CPU ";
	private static final String LACK_MEM = " MEM ";
	private static final String LACK_DISK = " DISK ";
	private static final String LACK_NET = " SLOT ";

	public static boolean checkResourceValid(ResourceAssignment taskAlloc,
			Integer task, DefaultTopologyAssignContext defaultContext,
			Map componentMap, String componentName,
			Set<String> canUsedSupervisorIds,
			Map<Integer, ResourceAssignment> alreadyAssign) {

		// it can skip this check
		// because dead task's old assignment can't pass isSupervisorAvailable
		// check
		// Set<Integer> deadTasks = defaultContext.getDeadTaskIds();
		// if (deadTasks.contains(task)) {
		// LOG.info(componentName + " task " + task +
		// " is dead, won't use old assignment");
		// return false;
		// }

		String allocSupervisorId = taskAlloc.getSupervisorId();
		if (canUsedSupervisorIds.contains(allocSupervisorId) == false) {
			// userdefine's supervisor can't be used
			LOG.info(componentMap + " task " + task + " can't use supervisor "
					+ allocSupervisorId);
			return false;
		}

		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();

		SupervisorInfo supervisorInfo = cluster.get(allocSupervisorId);
		if (supervisorInfo == null) {
			LOG.info(componentName + " task " + task
					+ " supervisorId is invalid " + allocSupervisorId);
			return false;
		}

		String lackingResource = isSupervisorAvailable(defaultContext,
				supervisorInfo, taskAlloc);
		if (lackingResource != null) {
			StringBuilder sb = new StringBuilder();
			sb.append("Supervisor " + allocSupervisorId);
			sb.append(" no enough resource for ");
			sb.append(componentName + " task " + task + ",");
			sb.append(taskAlloc);
			sb.append("\n\n");
			sb.append("the lacking resource is :");
			sb.append(lackingResource);
			LOG.info(sb.toString());
			return false;
		}

		if (isIsolate(task, allocSupervisorId, componentName, componentMap,
				defaultContext, alreadyAssign) == false) {
			StringBuilder sb = new StringBuilder();
			sb.append("Supervisor " + allocSupervisorId);
			sb.append(" TASK_ON_DIFFERENT_NODE fail ");
			sb.append(componentName + " task" + task + ",");
			sb.append(taskAlloc);
			LOG.info(sb.toString());
			return false;
		}

		return true;
	}

	public static String isSupervisorAvailable(
			DefaultTopologyAssignContext defaultContext,
			SupervisorInfo supervisorInfo, ResourceAssignment taskAlloc) {
		StringBuilder result = new StringBuilder();
		boolean isCpuAvailable = supervisorInfo.getCpuPool().isAvailable(
				taskAlloc.getCpuSlotNum(), defaultContext);
		if (isCpuAvailable == false) {
			result.append(LACK_CPU);
		}
		boolean isMemAvailable = supervisorInfo.getMemPool().isAvailable(
				taskAlloc.getMemSlotNum(), defaultContext);
		if (isMemAvailable == false) {
			result.append(LACK_MEM);
		}
		boolean isDiskAvailable = supervisorInfo.getDiskPool().isAvailable(
				taskAlloc.getDiskSlot(), defaultContext);
		if (isDiskAvailable == false) {
			result.append(LACK_DISK);
		}
		boolean isPortAvailable = supervisorInfo.getNetPool().isAvailable(
				taskAlloc.getPort(), defaultContext);
		if (isPortAvailable == false) {
			result.append(LACK_NET);
		}

		if (result.length() == 0)
			return null;
		else
			return result.toString();
	}

	public static String isSupervisorAvailable(
			DefaultTopologyAssignContext defaultContext,
			SupervisorInfo supervisorInfo, TaskAllocResource taskAlloc) {

		StringBuilder result = new StringBuilder();
		boolean isCpuAvailable = supervisorInfo.getCpuPool().isAvailable(
				taskAlloc.getCpuSlotNum(), defaultContext);
		if (isCpuAvailable == false) {
			result.append(LACK_CPU);
		}
		boolean isMemAvailable = supervisorInfo.getMemPool().isAvailable(
				taskAlloc.getMemSlotNum(), defaultContext);
		if (isMemAvailable == false) {
			result.append(LACK_MEM);
		}

		if (taskAlloc.isAllocDisk()
				&& supervisorInfo.getDiskPool().getLeftNum() < 1) {
			result.append(LACK_DISK);
		}

		if (supervisorInfo.getNetPool().getLeftNum() < 1) {
			result.append(LACK_NET);
		}

		if (result.length() == 0)
			return null;
		else
			return result.toString();
	}

	public static void allocResource(
			DefaultTopologyAssignContext defaultContext,
			ResourceAssignment taskAlloc) {
		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();

		SupervisorInfo supervisorInfo = cluster
				.get(taskAlloc.getSupervisorId());
		if (supervisorInfo == null) {
			LOG.warn("No supervisor of " + taskAlloc.getSupervisorId() + ","
					+ defaultContext.toDetailString());
			return;
		}

		/**
		 * Don't alloc port, port will be assign by mergePort function
		 */
		supervisorInfo.getCpuPool().alloc(taskAlloc.getCpuSlotNum(),
				defaultContext);
		supervisorInfo.getMemPool().alloc(taskAlloc.getMemSlotNum(),
				defaultContext);
		supervisorInfo.getDiskPool().alloc(taskAlloc.getDiskSlot(),
				defaultContext);

	}

	public static ResourceAssignment allocResource(
			DefaultTopologyAssignContext defaultContext,
			TaskAllocResource taskAlloc, String supervisorId) {

		String diskSlot = null;

		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();

		SupervisorInfo supervisorInfo = cluster.get(supervisorId);
		if (supervisorInfo == null) {
			StringBuilder sb = new StringBuilder();
			sb.append("No supervisor of " + supervisorId + ",");
			sb.append("\n\n");
			sb.append(defaultContext.toDetailString());
			LOG.warn(sb.toString());
			throw new FailedAssignTopologyException(sb.toString());
		}

		/**
		 * Don't alloc port, port will be assign by mergePort function
		 */
		supervisorInfo.getCpuPool().alloc(taskAlloc.getCpuSlotNum(),
				defaultContext);
		supervisorInfo.getMemPool().alloc(taskAlloc.getMemSlotNum(),
				defaultContext);

		if (taskAlloc.isAllocDisk()) {
			diskSlot = supervisorInfo.getDiskPool().alloc(defaultContext);
		}

		ResourceAssignment taskResource = new ResourceAssignment();
		taskResource.setSupervisorId(supervisorId);
		taskResource.setDiskSlot(diskSlot);
		taskResource.setCpuSlotNum(taskAlloc.getCpuSlotNum());
		taskResource.setMemSlotNum(taskAlloc.getMemSlotNum());
		taskResource.setPort(null);
		return taskResource;
	}

	/**
	 * if the task has been set TASK_RUN_ON_DIFFERENT_NODE the component's task
	 * should be run on different node
	 * 
	 * @param task
	 * @param assignSupervisorId
	 * @param componentName
	 * @param alreadyAssign
	 * @return
	 */
	public static boolean isIsolate(Integer task, String assignSupervisorId,
			String componentName, Map componentMap,
			DefaultTopologyAssignContext defaultContext,
			Map<Integer, ResourceAssignment> alreadyAssign) {

		boolean needCheck = ConfigExtension.isTaskOnDifferentNode(componentMap);
		if (needCheck == false) {
			return true;
		}

		Map<String, List<Integer>> compoentTaskList = defaultContext
				.getComponentTasks();
		List<Integer> taskList = compoentTaskList.get(componentName);
		if (taskList == null) {
			LOG.warn("No tasks of " + componentName);
			return true;
		}

		Set<String> usedSupervisorIds = new HashSet<String>();
		for (Integer taskEntry : taskList) {
			ResourceAssignment assignment = alreadyAssign.get(taskEntry);
			if (assignment == null) {
				continue;
			}

			usedSupervisorIds.add(assignment.getSupervisorId());
		}

		if (usedSupervisorIds.contains(assignSupervisorId)) {
			return false;
		} else {
			return true;
		}
	}

	public static ResourceAssignment normalizeResourceAssignment(
			DefaultTopologyAssignContext defaultContext,
			ResourceAssignment taskAlloc, boolean checkHostname) {
		if (taskAlloc.isCpuMemInvalid() == true) {
			return null;
		}

		if (checkHostname == false) {
			return taskAlloc;
		}
		if (taskAlloc.getSupervisorId() == null
				&& taskAlloc.getHostname() == null) {
			return null;
		}

		if (taskAlloc.getSupervisorId() != null) {
			return taskAlloc;
		} else {
			Map<String, List<String>> hostToSid = defaultContext.getHostToSid();
			List<String> supervisorIds = hostToSid.get(taskAlloc.getHostname());
			if (supervisorIds == null || supervisorIds.isEmpty()) {
				return null;
			}

			taskAlloc.setSupervisorId(supervisorIds.get(0));
		}

		return taskAlloc;
	}

	public static ResourceAssignment getUserDefineAlloc(Integer task,
			DefaultTopologyAssignContext defaultContext, Map componentMap,
			String componentName, boolean checkHostname) {
		Map<String, List<Integer>> componentTaskMap = defaultContext
				.getComponentTasks();

		List<Object> userDefineAssigns = JStormServerConfig
				.getUserDefineAssignmentFromJson(componentMap);
		if (userDefineAssigns == null) {
			return null;
		}

		List<Integer> componentTasks = componentTaskMap.get(componentName);
		if (componentTasks == null) {
			// this shouldn't occur
			LOG.warn(componentMap + " no tasks of " + componentName);
			return null;
		}

		int index = componentTasks.indexOf(task);
		if (index < 0) {
			// this shouldn't occur
			LOG.warn(componentName + " no task of " + task);
			return null;
		}

		if (userDefineAssigns.size() <= index) {
			LOG.info(componentName + " task " + task
					+ " no user define ResourceAssign");
			return null;
		}

		Object obj = userDefineAssigns.get(index);
		ResourceAssignment userDefineAssign = ResourceAssignment
				.parseFromObj(obj);
		if (userDefineAssign == null) {
			LOG.info(componentName + " index task " + index
					+ " isn't ResourceAssignment " + obj.toString());
			return null;
		}

		ResourceAssignment taskAlloc = normalizeResourceAssignment(
				defaultContext, userDefineAssign, checkHostname);
		if (taskAlloc == null) {
			LOG.info(componentName + " index task " + index
					+ " ResourceAssignment is invalid "
					+ (ResourceAssignment) userDefineAssign);
			return null;
		}

		return taskAlloc;
	}

	public static TaskAllocResource getTaskAlloc(Integer task,
			DefaultTopologyAssignContext defaultContext, Map componentMap,
			String componentName) {

		ResourceAssignment useDefineAlloc = getUserDefineAlloc(task,
				defaultContext, componentMap, componentName, false);
		if (useDefineAlloc != null) {
			return new TaskAllocResource(useDefineAlloc);
		}

		int cpuSlotNum = ConfigExtension.getCpuSlotsPerTask(componentMap);
		int memSlotNum = ConfigExtension.getMemSlotPerTask(componentMap);
		boolean diskAlloc = ConfigExtension.isTaskAllocDisk(componentMap);

		return new TaskAllocResource(diskAlloc, cpuSlotNum, memSlotNum);
	}
}
