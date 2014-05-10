package com.alibaba.jstorm.schedule.default_assign;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;

public class PostAssignTaskPort implements IPostAssignTask {

	@Override
	public void postAssign(DefaultTopologyAssignContext defaultContext,
			Map<Integer, ResourceAssignment> newAssigns, int allocWorkerNum) {
		assignTaskPort(defaultContext, newAssigns, allocWorkerNum);
	}

	/**
	 * get to know how many workers for every supervisor
	 * 
	 * @param defaultContext
	 * @param newAssigns
	 * @param allocWorkerNum
	 * @param supervisorTasks
	 * @param supervisorAssignSlotNumMap
	 */
	public void assignWorkerNumToSupervisor(
			DefaultTopologyAssignContext defaultContext,
			Map<Integer, ResourceAssignment> newAssigns, int allocWorkerNum,
			Map<String, List<Integer>> supervisorTasks,
			Map<String, Integer> supervisorAssignSlotNumMap) {
		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();

		Map<String, Integer> supervisorTaskNumMap = new HashMap<String, Integer>();

		for (Entry<Integer, ResourceAssignment> entry : newAssigns.entrySet()) {
			Integer taskId = entry.getKey();
			ResourceAssignment assign = entry.getValue();
			String sid = assign.getSupervisorId();

			Integer num = supervisorTaskNumMap.get(sid);
			if (num == null) {
				num = Integer.valueOf(0);
			}
			num = num + 1;
			supervisorTaskNumMap.put(sid, num);

			List<Integer> taskList = supervisorTasks.get(sid);
			if (taskList == null) {
				taskList = new ArrayList<Integer>();
				supervisorTasks.put(sid, taskList);
			}
			taskList.add(taskId);
		}

		final Map<String, Integer> supervisorSlotNumMap = new HashMap<String, Integer>();
		for (String sid : supervisorTaskNumMap.keySet()) {
			SupervisorInfo supervisorInfo = cluster.get(sid);
			supervisorSlotNumMap.put(sid, supervisorInfo.getNetPool()
					.getLeftNum());
		}

		List<String> sortSupervisorList = new ArrayList<String>();
		sortSupervisorList.addAll(supervisorSlotNumMap.keySet());

		Collections.sort(sortSupervisorList, new Comparator<String>() {

			@Override
			public int compare(String first, String second) {
				Integer firstWeight = supervisorSlotNumMap.get(first);
				Integer secondWeight = supervisorSlotNumMap.get(second);

				return secondWeight - firstWeight;
			}

		});

		// get to know how many workers every supervisor
		int index = 0;
		int tmp = Math.min(allocWorkerNum, newAssigns.size());
		while (tmp > 0 && sortSupervisorList.size() > 0) {
			if (index >= sortSupervisorList.size()) {
				index = 0;
			}

			String supervisorId = sortSupervisorList.get(index);
			Integer assignNum = supervisorAssignSlotNumMap.get(supervisorId);
			if (assignNum == null) {
				assignNum = Integer.valueOf(0);
			}

			Integer taskNum = supervisorTaskNumMap.get(supervisorId);

			if (taskNum <= assignNum) {
				sortSupervisorList.remove(index);
				continue;
			}

			Integer slotNum = supervisorSlotNumMap.get(supervisorId);

			if (slotNum > assignNum) {

				assignNum = assignNum + 1;
				supervisorAssignSlotNumMap.put(supervisorId, assignNum);

				index++;
				tmp--;
			}else {
				sortSupervisorList.remove(index);
				continue;
			}

			
		}
	}

	public Integer selectLeastPort(Map<Integer, List<Integer>> preAssignPorts) {
		Integer port = null;
		int portLeastTaskNum = Integer.MAX_VALUE;

		for (Entry<Integer, List<Integer>> entry : preAssignPorts.entrySet()) {
			int size = entry.getValue().size();
			if (size < portLeastTaskNum) {
				portLeastTaskNum = size;
				port = entry.getKey();
			}
		}

		return port;
	}

	/**
	 * Assign port to one supervisor's tasks
	 * 
	 * @param defaultContext
	 * @param taskList
	 * @param workerNum
	 * @param newAssigns
	 * @param supervisorInfo
	 */
	public void assignTaskPort(DefaultTopologyAssignContext defaultContext,
			List<Integer> taskList, Integer workerNum,
			Map<Integer, ResourceAssignment> newAssigns,
			SupervisorInfo supervisorInfo) {

		if (taskList.size() <= workerNum) {
			// workerNum is enough to taskNum
			for (Integer task : taskList) {
				ResourceAssignment assignment = newAssigns.get(task);

				if (assignment.getPort() != null) {
					supervisorInfo.getNetPool().alloc(assignment.getPort(),
							defaultContext);
				} else {
					Integer port = supervisorInfo.getNetPool().alloc(
							defaultContext);
					assignment.setPort(port);
				}

			}
		} else {
			// preAssign's port to taskList
			Map<Integer, List<Integer>> preAssignPorts = new TreeMap<Integer, List<Integer>>();
			for (Integer task : taskList) {
				ResourceAssignment assignment = newAssigns.get(task);
				if (assignment.getPort() != null) {
					Integer port = assignment.getPort();
					List<Integer> slotList = preAssignPorts.get(port);

					if (slotList == null) {
						slotList = new ArrayList<Integer>();
						preAssignPorts.put(port, slotList);
					}
					slotList.add(task);
				}

			}

			if (preAssignPorts.keySet().size() > workerNum) {
				// reassign all port
				List<Integer> assignPorts = new ArrayList<Integer>();
				for (int i = 0; i < workerNum; i++) {
					Integer port = supervisorInfo.getNetPool().alloc(
							defaultContext);
					assignPorts.add(port);
				}

				for (int i = 0; i < taskList.size(); i++) {
					ResourceAssignment assignment = newAssigns.get(taskList
							.get(i));

					assignment.setPort(assignPorts.get(i % assignPorts.size()));
				}
			} else {
				// all pre alloc port will be used
				for (Integer port : preAssignPorts.keySet()) {
					supervisorInfo.getNetPool().alloc(port, defaultContext);
				}

				for (int i = preAssignPorts.keySet().size(); i < workerNum; i++) {
					Integer port = supervisorInfo.getNetPool().alloc(
							defaultContext);
					preAssignPorts.put(port, new ArrayList<Integer>());
				}

				for (Integer task : taskList) {
					ResourceAssignment assignment = newAssigns.get(task);

					if (assignment.getPort() != null) {
						continue;
					}

					Integer port = selectLeastPort(preAssignPorts);
					assignment.setPort(port);

					List<Integer> portTaskList = preAssignPorts.get(port);
					portTaskList.add(task);
				}
			}
		}
	}

	public void assignTaskPort(DefaultTopologyAssignContext defaultContext,
			Map<String, Integer> supervisorAssignSlotNumMap,
			Map<String, List<Integer>> supervisorTasks,
			Map<Integer, ResourceAssignment> newAssigns) {
		Map<String, SupervisorInfo> cluster = defaultContext.getCluster();
		for (Entry<String, List<Integer>> entry : supervisorTasks.entrySet()) {
			String sid = entry.getKey();
			List<Integer> taskList = entry.getValue();

			Integer workerNum = supervisorAssignSlotNumMap.get(sid);
			SupervisorInfo supervisorInfo = cluster.get(sid);

			assignTaskPort(defaultContext, taskList, workerNum, newAssigns,
					supervisorInfo);
		}
	}

	/**
	 * Assign port to every task
	 * 
	 * @param defaultContext
	 * @param newAssigns
	 * @param allocWorkerNum
	 */
	public void assignTaskPort(DefaultTopologyAssignContext defaultContext,
			Map<Integer, ResourceAssignment> newAssigns, int allocWorkerNum) {

		Map<String, List<Integer>> supervisorTasks = new HashMap<String, List<Integer>>();
		Map<String, Integer> supervisorAssignSlotNumMap = new HashMap<String, Integer>();

		assignWorkerNumToSupervisor(defaultContext, newAssigns, allocWorkerNum,
				supervisorTasks, supervisorAssignSlotNumMap);

		assignTaskPort(defaultContext, supervisorAssignSlotNumMap,
				supervisorTasks, newAssigns);
	}

}
