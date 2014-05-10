package com.alibaba.jstorm.daemon.nimbus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.ThriftResourceType;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.schedule.DefaultScheduler;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class NimbusUtils {

	private static Logger LOG = Logger.getLogger(NimbusUtils.class);

	/**
	 * add coustom KRYO serialization
	 * 
	 */
	private static Map mapifySerializations(List sers) {
		Map rtn = new HashMap();
		if (sers != null) {
			int size = sers.size();
			for (int i = 0; i < size; i++) {
				if (sers.get(i) instanceof Map) {
					rtn.putAll((Map) sers.get(i));
				} else {
					rtn.put(sers.get(i), null);
				}
			}

		}
		return rtn;
	}

	/**
	 * Normalize stormConf
	 * 
	 * 
	 * 
	 * @param conf
	 * @param stormConf
	 * @param topology
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static Map normalizeConf(Map conf, Map stormConf,
			StormTopology topology) throws Exception {

		List kryoRegisterList = new ArrayList();
		List kryoDecoratorList = new ArrayList();

		Map totalConf = new HashMap();
		totalConf.putAll(conf);
		totalConf.putAll(stormConf);

		Object totalRegister = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
		if (totalRegister != null) {
			LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
					+ ", TOPOLOGY_KRYO_REGISTER"
					+ totalRegister.getClass().getName());

			JStormUtils.mergeList(kryoRegisterList, totalRegister);
		}

		Object totalDecorator = totalConf.get(Config.TOPOLOGY_KRYO_DECORATORS);
		if (totalDecorator != null) {
			LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
					+ ", TOPOLOGY_KRYO_DECORATOR"
					+ totalDecorator.getClass().getName());
			JStormUtils.mergeList(kryoDecoratorList, totalDecorator);
		}

		Set<String> cids = ThriftTopologyUtils.getComponentIds(topology);
		for (Iterator it = cids.iterator(); it.hasNext();) {
			String componentId = (String) it.next();

			ComponentCommon common = ThriftTopologyUtils.getComponentCommon(
					topology, componentId);
			String json = common.get_json_conf();
			if (json == null) {
				continue;
			}
			Map mtmp = (Map) JStormUtils.from_json(json);
			if (mtmp == null) {
				StringBuilder sb = new StringBuilder();

				sb.append("Failed to deserilaize " + componentId);
				sb.append(" json configuration: ");
				sb.append(json);
				LOG.info(sb.toString());
				throw new Exception(sb.toString());
			}

			Object componentKryoRegister = mtmp
					.get(Config.TOPOLOGY_KRYO_REGISTER);

			if (componentKryoRegister != null) {
				LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
						+ ", componentId:" + componentId
						+ ", TOPOLOGY_KRYO_REGISTER"
						+ componentKryoRegister.getClass().getName());

				JStormUtils.mergeList(kryoRegisterList, componentKryoRegister);
			}

			Object componentDecorator = mtmp
					.get(Config.TOPOLOGY_KRYO_DECORATORS);
			if (componentDecorator != null) {
				LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
						+ ", componentId:" + componentId
						+ ", TOPOLOGY_KRYO_DECORATOR"
						+ componentDecorator.getClass().getName());
				JStormUtils.mergeList(kryoDecoratorList, componentDecorator);
			}

		}

		Map kryoRegisterMap = mapifySerializations(kryoRegisterList);
		List decoratorList = JStormUtils.distinctList(kryoDecoratorList);

		Integer ackerNum = JStormUtils.parseInt(totalConf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));
		if (ackerNum == null) {
			ackerNum = Integer.valueOf(0);
		}

		Map rtn = new HashMap();
		rtn.putAll(stormConf);
		rtn.put(Config.TOPOLOGY_KRYO_DECORATORS, decoratorList);
		rtn.put(Config.TOPOLOGY_KRYO_REGISTER, kryoRegisterMap);
		rtn.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
		rtn.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,
				totalConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
		return rtn;
	}

	public static Integer componentParalism(Map stormConf,
			ComponentCommon common) {
		Map mergeMap = new HashMap();
		mergeMap.putAll(stormConf);

		String jsonConfString = common.get_json_conf();
		if (jsonConfString != null) {
			Map componentMap = (Map) JStormUtils.from_json(jsonConfString);
			mergeMap.putAll(componentMap);
		}

		Integer taskNum = common.get_parallelism_hint();
		if (taskNum == null) {
			taskNum = Integer.valueOf(1);
		}

		// don't get taskNum from component configuraiton
		// skip .setTaskNum
		// Integer taskNum = null;
		// Object taskNumObject = mergeMap.get(Config.TOPOLOGY_TASKS);
		// if (taskNumObject != null) {
		// taskNum = JStormUtils.parseInt(taskNumObject);
		// } else {
		// taskNum = common.get_parallelism_hint();
		// if (taskNum == null) {
		// taskNum = Integer.valueOf(1);
		// }
		// }

		Object maxTaskParalismObject = mergeMap
				.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
		if (maxTaskParalismObject == null) {
			return taskNum;
		} else {
			int maxTaskParalism = JStormUtils.parseInt(maxTaskParalismObject);

			return Math.min(maxTaskParalism, taskNum);
		}

	}

	public static void normalizeComponentResouce(Map componentMap,
			String componentName) {

		List<Object> userDefines = JStormServerConfig
				.getUserDefineAssignmentFromJson(componentMap);
		if (userDefines == null) {
			return;
		}

		int maxCpu = Integer.MIN_VALUE;
		int maxMem = Integer.MIN_VALUE;
		Boolean useDisk = null;
		for (int index = 0; index < userDefines.size(); index++) {
			ResourceAssignment userDefineAssign = ResourceAssignment
					.parseFromObj(userDefines.get(index));
			if (userDefineAssign == null) {
				String errMsg = componentName + " index task " + index
						+ " isn't ResourceAssignment ";
				LOG.warn(errMsg);
				throw new FailedAssignTopologyException(errMsg);
			}

			if (userDefineAssign.isCpuMemInvalid()) {
				StringBuilder sb = new StringBuilder();

				sb.append(componentName);
				sb.append(" index task " + index);
				sb.append(" ResourceAssignment is invalid ");
				sb.append(userDefineAssign);

				LOG.warn(sb.toString());
				throw new FailedAssignTopologyException(sb.toString());
			}

			if (maxCpu < userDefineAssign.getCpuSlotNum()) {
				maxCpu = userDefineAssign.getCpuSlotNum();
			}

			if (maxMem < userDefineAssign.getMemSlotNum()) {
				maxMem = userDefineAssign.getMemSlotNum();
			}

			if (useDisk == null) {
				useDisk = !StringUtils.isBlank(userDefineAssign.getDiskSlot());
			} else if (useDisk == StringUtils.isBlank(userDefineAssign
					.getDiskSlot())) {
				StringBuilder sb = new StringBuilder();

				sb.append("No all tasks of ");
				sb.append(componentName);
				sb.append(" alloc disk, ");
				sb.append(" All tasks' alloc disk setting should be same.\n");
				sb.append(userDefines);

				LOG.warn(sb.toString());
				throw new FailedAssignTopologyException(sb.toString());
			}
		}

		int setTaskCpuSlot = ConfigExtension.getCpuSlotsPerTask(componentMap);
		if (maxCpu != setTaskCpuSlot) {
			StringBuilder sb = new StringBuilder();

			sb.append(componentName);
			sb.append(" CpuSlotsPerTask setting is  " + setTaskCpuSlot);
			sb.append(", but UserDefineAssignment cpu slot setting is "
					+ maxCpu);

			LOG.warn(sb.toString());

			ConfigExtension.setCpuSlotsPerTask(componentMap, maxCpu);
		}

		int setTaskMemSlot = ConfigExtension.getMemSlotPerTask(componentMap);
		if (maxMem != setTaskMemSlot) {
			StringBuilder sb = new StringBuilder();

			sb.append(componentName);
			sb.append(" MEM_SLOTS_PER_TASK setting is  " + setTaskMemSlot);
			sb.append(", but USE_USERDEFINE_ASSIGNMENT memory slot setting is "
					+ maxMem);

			LOG.warn(sb.toString());
			ConfigExtension.setMemSlotPerTask(componentMap, maxMem);
		}

		boolean setUseDisk = ConfigExtension.isTaskAllocDisk(componentMap);
		if (setUseDisk != useDisk) {
			StringBuilder sb = new StringBuilder();

			sb.append(componentName);
			sb.append(" ConfigExtension.isTaskAllocDisk setting is  "
					+ setTaskMemSlot);
			sb.append(", but USE_USERDEFINE_ASSIGNMENT disk slot setting is "
					+ useDisk);

			LOG.warn(sb.toString());
			ConfigExtension.setTaskAllocDisk(componentMap, useDisk);
		}

		return;
	}

	/**
	 * finalize component's task paralism
	 * 
	 * @param topology
	 * @return
	 */
	public static StormTopology normalizeTopology(Map stormConf,
			StormTopology topology) {
		StormTopology ret = topology.deepCopy();

		boolean isSingleNode = ConfigExtension.isUseSingleNode(stormConf);

		Map<String, Object> components = ThriftTopologyUtils.getComponents(ret);
		for (Entry<String, Object> entry : components.entrySet()) {
			Object component = entry.getValue();
			String componentName = entry.getKey();

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

			Map componentMap = new HashMap();

			String jsonConfString = common.get_json_conf();
			if (jsonConfString != null) {
				componentMap
						.putAll((Map) JStormUtils.from_json(jsonConfString));
			}

			Integer taskNum = componentParalism(stormConf, common);

			componentMap.put(Config.TOPOLOGY_TASKS, taskNum);
			// change the executor's task number
			common.set_parallelism_hint(taskNum);
			LOG.info("Set " + componentName + " parallelism " + taskNum);

			normalizeComponentResouce(componentMap, componentName);

			boolean allocDisk = ConfigExtension.isTaskAllocDisk(componentMap);
			if (allocDisk == true) {
				// alloc disk slot
				LOG.info("Topology:" + stormConf.get(Config.TOPOLOGY_NAME)
						+ " alloc disk slot");

				// if alloc disk, it will automatically set the following
				ConfigExtension.setTaskOnDifferentNode(componentMap, true);
				ConfigExtension.setUseOldAssignment(componentMap, true);
			}

			if (isSingleNode == true) {
				boolean isTaskOnDiff = ConfigExtension
						.isTaskOnDifferentNode(componentMap);

				if (isTaskOnDiff == true && taskNum > 1) {
					StringBuilder sb = new StringBuilder();

					sb.append(componentName);
					sb.append(" has set TASK_ON_DIFFERENT_NODE ");
					sb.append(" which is conflict with global setting USE_SINGLE_NODE ");

					LOG.info(sb.toString());

					throw new FailedAssignTopologyException(sb.toString());
				}
			}

			common.set_json_conf(JStormUtils.to_json(componentMap));
		}

		return ret;
	}

	/**
	 * clean the topology which is in ZK but not in local dir
	 * 
	 * @throws Exception
	 * 
	 */
	public static void cleanupCorruptTopologies(NimbusData data)
			throws Exception {

		StormClusterState stormClusterState = data.getStormClusterState();

		// get /local-storm-dir/nimbus/stormdist path
		String master_stormdist_root = StormConfig.masterStormdistRoot(data
				.getConf());

		// listdir /local-storm-dir/nimbus/stormdist
		List<String> code_ids = PathUtils
				.read_dir_contents(master_stormdist_root);

		// get topology in ZK /storms
		List<String> active_ids = data.getStormClusterState().active_storms();
		if (active_ids != null && active_ids.size() > 0) {
			if (code_ids != null) {
				// clean the topology which is in ZK but not in local dir
				active_ids.removeAll(code_ids);
			}

			for (String corrupt : active_ids) {
				LOG.info("Corrupt topology "
						+ corrupt
						+ " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...");

				/**
				 * Just removing the /STORMS is enough
				 * 
				 */
				stormClusterState.remove_storm(corrupt);
			}
		}

		LOG.info("Successfully cleanup all old toplogies");

	}

	public static void synchronizeGroupToTopology(NimbusData data) {

		try {
			if (data.isGroupMode()) {
				List<String> active_ids = data.getStormClusterState()
						.active_storms();
				Map<String, Map<String, Map<ThriftResourceType, Integer>>> groupToTopology = data
						.getGroupToTopology();
				for (String active_id : active_ids) {
					Assignment assignment = data.getStormClusterState()
							.assignment_info(active_id, null);
					StormBase stormBase = data.getStormClusterState()
							.storm_base(active_id, null);
					String group = stormBase.getGroup();
					if (group == null)
						continue;
					Map<String, Map<ThriftResourceType, Integer>> topologys = groupToTopology
							.get(group);
					if (topologys == null) {
						topologys = new HashMap<String, Map<ThriftResourceType, Integer>>();
						groupToTopology.put(group, topologys);
					}
					Map<ThriftResourceType, Integer> topology = topologys
							.get(stormBase.getStormName());
					/**
					 * @@@ changed
					 */
					topology = getTopologyResource(topology,
							assignment.getTaskToResource());
					topologys.put(stormBase.getStormName(), topology);
					
				}
			}
		} catch (Exception e) {
			LOG.error("synchronizeGroupToTopology error!", e);
			throw new RuntimeException();
		}
	}

	public static void synchronizeGroupToResource(NimbusData data) {
		if (data.isGroupMode()) {
			Map<String, Map<ThriftResourceType, Integer>> groupToUsedResource = data
					.getGroupToUsedResource();
			Map<String, Map<String, Map<ThriftResourceType, Integer>>> groupToTopology = data
					.getGroupToTopology();
			for (String group : groupToTopology.keySet()) {
				Map<ThriftResourceType, Integer> usedResource = new HashMap<ThriftResourceType, Integer>();
				usedResource.put(ThriftResourceType.CPU, 0);
				usedResource.put(ThriftResourceType.MEM, 0);
				usedResource.put(ThriftResourceType.DISK, 0);
				usedResource.put(ThriftResourceType.NET, 0);
				groupToUsedResource.put(group, usedResource);
				Map<String, Map<ThriftResourceType, Integer>> topologys = groupToTopology
						.get(group);
				if (topologys == null)
					continue;
				for (Entry<String, Map<ThriftResourceType, Integer>> entry : topologys
						.entrySet()) {
					Map<ThriftResourceType, Integer> topologyResource = entry
							.getValue();
					for (Entry<ThriftResourceType, Integer> resourceEntry : topologyResource
							.entrySet()) {
						usedResource.put(resourceEntry.getKey(),
								usedResource.get(resourceEntry.getKey())
										+ resourceEntry.getValue());
					}
				}
			}
		}
	}

	public static String getGroupName(NimbusData data, String topologyId)
			throws Exception {
		if (!data.isGroupMode())
			return null;
		StormClusterState stormClusterState = data.getStormClusterState();
		StormBase base = stormClusterState.storm_base(topologyId, null);
		if (base == null) {
			return null;
		}
		
		return base.getGroup();
	}

	public static void releaseGroupResource(NimbusData data,
			String topologyName, String group) throws Exception {
		if (!data.isGroupMode())
			return;
		if (group == null)
			return;
		Map<ThriftResourceType, Integer> topologyResource = data
				.getGroupToTopology().get(group).remove(topologyName);
		Map<ThriftResourceType, Integer> usedResource = data
				.getGroupToUsedResource().get(group);
		for (Entry<ThriftResourceType, Integer> entry : topologyResource
				.entrySet()) {
			usedResource.put(entry.getKey(), usedResource.get(entry.getKey())
					- entry.getValue());
		}
	}

	public static Map<ThriftResourceType, Integer> getTopologyResource(
			Map<ThriftResourceType, Integer> result,
			Map<Integer, ResourceAssignment> assignment) {
		if (result == null)
			result = new HashMap<ThriftResourceType, Integer>();
		int cpuNum = 0;
		int memNum = 0;
		
		Set<String> disk = new HashSet<String>();
		Set<String> slot = new HashSet<String>();
		for (Entry<Integer, ResourceAssignment> entry : assignment.entrySet()) {
			
			ResourceAssignment resource = entry.getValue();
			
			cpuNum += resource.getCpuSlotNum();
			memNum += resource.getMemSlotNum();
			
			if (resource.getDiskSlot() != null) {
				disk.add(resource.getDiskSlot());
			}
			slot.add(String.valueOf(resource.getPort())
					+ resource.getSupervisorId());
		}
		
		result.put(ThriftResourceType.CPU, cpuNum);
		result.put(ThriftResourceType.MEM, memNum);
		result.put(ThriftResourceType.DISK, disk.size());
		result.put(ThriftResourceType.NET, slot.size());

		return result;
	}

	public static boolean isTaskDead(NimbusData data, String topologyId,
			Integer taskId) {
		String idStr = " topology:" + topologyId + ",taskid:" + taskId;

		Integer zkReportTime = null;

		StormClusterState stormClusterState = data.getStormClusterState();
		TaskHeartbeat zkTaskHeartbeat = null;
		try {
			zkTaskHeartbeat = stormClusterState.task_heartbeat(topologyId,
					taskId);
			if (zkTaskHeartbeat != null) {
				zkReportTime = zkTaskHeartbeat.getTimeSecs();
			}
		} catch (Exception e) {
			LOG.error("Failed to get ZK task hearbeat " + idStr);
			return true;
		}

		Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache()
				.get(topologyId);
		if (taskHBs == null) {
			LOG.info("No task heartbeat cache " + topologyId);

			// update task hearbeat cache
			taskHBs = new HashMap<Integer, TkHbCacheTime>();

			data.getTaskHeartbeatsCache().put(topologyId, taskHBs);
		}

		TkHbCacheTime taskHB = taskHBs.get(taskId);
		if (taskHB == null) {
			LOG.info("No task heartbeat cache " + idStr);

			if (zkTaskHeartbeat == null) {
				LOG.info("No ZK task hearbeat " + idStr);
				return true;
			}

			taskHB = new TkHbCacheTime();
			taskHB.update(zkTaskHeartbeat);

			taskHBs.put(taskId, taskHB);

			return false;
		}

		if (zkReportTime == null) {
			LOG.debug("No ZK task heartbeat " + idStr);
			// Task hasn't finish init
			int nowSecs = TimeUtils.current_time_secs();
			int assignSecs = taskHB.getTaskAssignedTime();

			int waitInitTimeout = JStormUtils.parseInt(data.getConf().get(
					Config.NIMBUS_TASK_LAUNCH_SECS));

			if (nowSecs - assignSecs > waitInitTimeout) {
				LOG.info(idStr + " failed to init ");
				return true;
			} else {
				return false;
			}

		}

		// the left is zkReportTime isn't null
		// task has finished initialization
		int nimbusTime = taskHB.getNimbusTime();
		int reportTime = taskHB.getTaskReportedTime();

		int nowSecs = TimeUtils.current_time_secs();
		if (nimbusTime == 0) {
			// taskHB no entry, first time
			// update taskHB
			taskHB.setNimbusTime(nowSecs);
			taskHB.setTaskReportedTime(zkReportTime);

			LOG.info("Update taskheartbeat to nimbus cache " + idStr);
			return false;
		}

		if (reportTime != zkReportTime.intValue()) {
			// zk has been updated the report time
			taskHB.setNimbusTime(nowSecs);
			taskHB.setTaskReportedTime(zkReportTime);

			LOG.debug(idStr + ",nimbusTime " + nowSecs + ",zkReport:"
					+ zkReportTime + ",report:" + reportTime);
			return false;
		}

		// the following is (zkReportTime == reportTime)
		int taskHBTimeout = JStormUtils.parseInt(data.getConf().get(
				Config.NIMBUS_TASK_TIMEOUT_SECS));

		if (nowSecs - nimbusTime > taskHBTimeout) {
			// task is dead
			long ts = ((long)nimbusTime) * 1000;
			Date lastTaskHBDate = new Date(ts);
			StringBuilder sb = new StringBuilder();
			
			sb.append(idStr);
			sb.append(" last tasktime is ");
			sb.append(nimbusTime);
			sb.append(":").append(lastTaskHBDate);
			sb.append(",current ");
			sb.append(nowSecs);
			sb.append(":").append(new Date(((long)nowSecs) * 1000));
			LOG.info(sb.toString());
			return true;
		}

		return false;

	}

	public static void updateTaskHbStartTime(NimbusData data,
			Assignment assignment, String topologyId) {
		Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache()
				.get(topologyId);

		if (taskHBs == null) {
			taskHBs = new HashMap<Integer, TkHbCacheTime>();
			data.getTaskHeartbeatsCache().put(topologyId, taskHBs);
		}

		Map<Integer, Integer> taskStartTimes = assignment
				.getTaskStartTimeSecs();
		for (Entry<Integer, Integer> entry : taskStartTimes.entrySet()) {
			Integer taskId = entry.getKey();
			Integer taskStartTime = entry.getValue();

			TkHbCacheTime taskHB = taskHBs.get(taskId);
			if (taskHB == null) {
				taskHB = new TkHbCacheTime();
				taskHBs.put(taskId, taskHB);
			}

			taskHB.setTaskAssignedTime(taskStartTime);
		}

		return;
	}

	public static <T> void transitionName(NimbusData data, String topologyName,
			boolean errorOnNoTransition, StatusType transition_status,
			T... args) throws Exception {
		StormClusterState stormClusterState = data.getStormClusterState();
		String topologyId = Cluster.get_topology_id(stormClusterState,
				topologyName);
		if (topologyId == null) {
			throw new NotAliveException(topologyName);
		}
		transition(data, topologyId, errorOnNoTransition, transition_status,
				args);
	}

	public static <T> void transition(NimbusData data, String topologyid,
			boolean errorOnNoTransition, StatusType transition_status,
			T... args) {
		try {
			data.getStatusTransition().transition(topologyid,
					errorOnNoTransition, transition_status, args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to do status transition,", e);
		}
	}

	public static IScheduler mkScheduler(Map conf, INimbus inimbus) {
		if (inimbus != null && inimbus.getForcedScheduler() != null)
			return inimbus.getForcedScheduler();
		else if (conf.get(Config.STORM_SCHEDULER) != null) {
			IScheduler result = null;
			try {
				result = (IScheduler) Class.forName(
						(String) conf.get(Config.STORM_SCHEDULER))
						.newInstance();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				LOG.error(Config.STORM_SCHEDULER
						+ " : Scheduler initialize error!");
				throw new RuntimeException("Fail to new Scheduler");
			}
			return result;
		} else {
			return new DefaultScheduler();
		}
	}

	public static TopologySummary mkTopologySummary(Assignment assignment,
			String topologyId, String topologyName, String status,
			int uptime_secs, String group) {
		Map<Integer, ResourceAssignment> taskAssignments = assignment
				.getTaskToResource();
		Map<WorkerSlot, List<Integer>> workerTaskList = Assignment
				.getWorkerTasks(taskAssignments);
		Set<WorkerSlot> workers = workerTaskList.keySet();

		int num_workers = workers.size();
		int num_tasks = 0;
		int num_cpu = 0;
		int num_mem = 0;
		int num_disk = 0;

		for (Entry<Integer, ResourceAssignment> entry : taskAssignments
				.entrySet()) {
			Integer task = entry.getKey();
			ResourceAssignment resourceAssignment = entry.getValue();

			num_tasks++;
			num_cpu += resourceAssignment.getCpuSlotNum();
			num_mem += resourceAssignment.getMemSlotNum();

			if (resourceAssignment.getDiskSlot() != null) {
				num_disk++;
			}

		}

		TopologySummary ret = new TopologySummary(topologyId, topologyName,
				status, uptime_secs, num_tasks, num_workers, num_cpu, num_mem,
				num_disk, group);

		return ret;
	}

	public static SupervisorSummary mkSupervisorSummary(
			SupervisorInfo supervisorInfo, String supervisorId) {
		SupervisorSummary summary = new SupervisorSummary(
				supervisorInfo.getHostName(), supervisorId,
				supervisorInfo.getUptimeSecs(), supervisorInfo.getNetPool()
						.getTotalNum(), supervisorInfo.getNetPool()
						.getUsedNum(), supervisorInfo.getCpuPool()
						.getTotalNum(), supervisorInfo.getCpuPool()
						.getUsedNum(), supervisorInfo.getMemPool()
						.getTotalNum(), supervisorInfo.getMemPool()
						.getUsedNum(), supervisorInfo.getDiskPool()
						.getTotalNum(), supervisorInfo.getDiskPool()
						.getUsedNum());

		return summary;
	}

	public static List<SupervisorSummary> mkSupervisorSummaries(
			Map<String, SupervisorInfo> supervisorInfos,
			Map<String, Assignment> assignments) {

		for (Entry<String, Assignment> entry : assignments.entrySet()) {
			Map<Integer, ResourceAssignment> taskToResource = entry.getValue()
					.getTaskToResource();

			for (Entry<Integer, ResourceAssignment> resourceEntry : taskToResource
					.entrySet()) {
				ResourceAssignment resource = resourceEntry.getValue();

				String supervisorId = resource.getSupervisorId();
				SupervisorInfo supervisorInfo = supervisorInfos
						.get(supervisorId);
				if (supervisorInfo == null) {
					continue;
				}

				supervisorInfo.allocResource(resource);
			}
		}

		List<SupervisorSummary> ret = new ArrayList<SupervisorSummary>();
		for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
			String supervisorId = entry.getKey();
			SupervisorInfo supervisorInfo = entry.getValue();

			SupervisorSummary summary = mkSupervisorSummary(supervisorInfo,
					supervisorId);

			ret.add(summary);
		}

		Collections.sort(ret, new Comparator<SupervisorSummary>() {

			@Override
			public int compare(SupervisorSummary o1, SupervisorSummary o2) {

				return o1.get_host().compareTo(o2.get_host());
			}

		});
		return ret;
	}

	public static TaskSummary mkSimpleTaskSummary(ResourceAssignment resource,
			int taskId, String component, String host, int uptime) {
		TaskSummary ret = new TaskSummary();

		ret.set_task_id(taskId);
		ret.set_component_id(component);
		ret.set_host(host);
		ret.set_cpu(resource.getCpuSlotNum());
		ret.set_mem(resource.getMemSlotNum());
		if (resource.getDiskSlot() == null) {
			ret.set_disk("");
		} else {
			ret.set_disk(resource.getDiskSlot());
		}
		ret.set_port(resource.getPort());
		ret.set_uptime_secs(uptime);
		ret.set_errors(new ArrayList<ErrorInfo>());

		return ret;
	}

	public static List<TaskSummary> mkTaskSummary(
			StormClusterState zkClusterState, Assignment assignment,
			Map<Integer, String> taskToComponent, String topologyId)
			throws Exception {

		List<TaskSummary> taskSummaries = new ArrayList<TaskSummary>();

		Map<Integer, ResourceAssignment> taskToResource = assignment
				.getTaskToResource();

		for (Entry<Integer, ResourceAssignment> entry : taskToResource
				.entrySet()) {
			Integer taskId = entry.getKey();
			ResourceAssignment resource = entry.getValue();

			TaskHeartbeat heartbeat = zkClusterState.task_heartbeat(topologyId,
					taskId);
			if (heartbeat == null) {
				LOG.warn("Topology " + topologyId + " task " + taskId
						+ " hasn't been started");
				continue;
			}

			List<TaskError> errors = zkClusterState.task_errors(topologyId,
					taskId);
			List<ErrorInfo> newErrors = new ArrayList<ErrorInfo>();

			if (errors != null) {
				int size = errors.size();
				for (int i = 0; i < size; i++) {
					TaskError e = (TaskError) errors.get(i);
					newErrors.add(new ErrorInfo(e.getError(), e.getTimSecs()));
				}
			}

			TaskSummary taskSummary = new TaskSummary();

			taskSummary.set_task_id(taskId);
			taskSummary.set_component_id(taskToComponent.get(taskId));
			taskSummary.set_host(assignment.getNodeHost().get(
					resource.getSupervisorId()));
			taskSummary.set_cpu(resource.getCpuSlotNum());
			taskSummary.set_mem(resource.getMemSlotNum());
			if (resource.getDiskSlot() == null) {
				taskSummary.set_disk("");
			} else {
				taskSummary.set_disk(resource.getDiskSlot());
			}

			taskSummary.set_port(resource.getPort());
			taskSummary.set_uptime_secs(heartbeat.getUptimeSecs());
			taskSummary.set_stats(heartbeat.getStats().getTaskStats());
			taskSummary.set_errors(newErrors);

			taskSummaries.add(taskSummary);
		}

		return taskSummaries;
	}
}
