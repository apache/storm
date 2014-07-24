package com.alibaba.jstorm.daemon.nimbus;

import java.io.IOException;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.ThriftResourceType;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.resource.ResourceType;
import com.alibaba.jstorm.resource.SlotResourcePool;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.DefaultTopologyScheduler;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.AssignmentBak;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class TopologyAssign implements Runnable {
	private final static Logger LOG = Logger.getLogger(TopologyAssign.class);

	/**
	 * private constructor function to avoid multiple instance
	 */
	private TopologyAssign() {

	}

	private static TopologyAssign instance = null;

	public static TopologyAssign getInstance() {
		synchronized (TopologyAssign.class) {
			if (instance == null) {
				instance = new TopologyAssign();
			}
			return instance;

		}
	}

	protected NimbusData nimbusData;

	protected Map<String, IToplogyScheduler> schedulers;
	
	private Thread thread;

	public static final String DEFAULT_SCHEDULER_NAME = "default";

	public void init(NimbusData nimbusData) {
		this.nimbusData = nimbusData;
		this.schedulers = new HashMap<String, IToplogyScheduler>();

		IToplogyScheduler defaultScheduler = new DefaultTopologyScheduler();
		defaultScheduler.prepare(nimbusData.getConf());

		schedulers.put(DEFAULT_SCHEDULER_NAME, defaultScheduler);

		thread = new Thread(this);
		thread.setName("TopologyAssign");
		thread.setDaemon(true);
		thread.start();
	}

	public void cleanup() {
		runFlag = false;
		thread.interrupt();
	}

	protected static LinkedBlockingQueue<TopologyAssignEvent> queue = new LinkedBlockingQueue<TopologyAssignEvent>();

	public static void push(TopologyAssignEvent event) {
		queue.offer(event);
	}

	volatile boolean runFlag = false;

	public void run() {
		LOG.info("TopologyAssign thread has been started");
		runFlag = true;

		while (runFlag) {
			TopologyAssignEvent event;
			try {
				event = queue.take();
			} catch (InterruptedException e1) {
				continue;
			}
			if (event == null) {
				continue;
			}
			
			boolean isSuccess = doTopologyAssignment(event);

			if (isSuccess == false) {
			} else {
				try {
					cleanupDisappearedTopology();
				} catch (Exception e) {
					LOG.error("Failed to do cleanup disappear topology ", e);
					continue;
				}
			}
		}

	}

	/**
	 * Create/Update topology assignment set topology status
	 * 
	 * @param event
	 * @return
	 */
	protected boolean doTopologyAssignment(TopologyAssignEvent event) {
		Assignment assignment = null;
		try {
			assignment = mkAssignment(event);

			setTopologyStatus(event);
		} catch (Throwable e) {
			LOG.error("Failed to assign topology " + event.getTopologyId(), e);
			event.fail(e.getMessage());
			return false;
		}

		backupAssignment(assignment, event);
		event.done();
		return true;
	}

	/**
	 * cleanup the topologies which are not in ZK /topology, but in other place
	 * 
	 * @param nimbusData
	 * @param active_topologys
	 * @throws Exception
	 */
	public void cleanupDisappearedTopology() throws Exception {
		StormClusterState clusterState = nimbusData.getStormClusterState();

		List<String> active_topologys = clusterState.active_storms();
		if (active_topologys == null) {
			return;
		}

		Set<String> cleanupIds = get_cleanup_ids(clusterState, active_topologys);

		for (String topologyId : cleanupIds) {

			LOG.info("Cleaning up " + topologyId);

			// remove ZK nodes /taskbeats/topologyId and
			// /taskerror/topologyId
			clusterState.teardown_heartbeats(topologyId);
			clusterState.teardown_task_errors(topologyId);

			//
			nimbusData.getTaskHeartbeatsCache().remove(topologyId);

			// get /nimbus/stormdist/topologyId
			String master_stormdist_root = StormConfig.masterStormdistRoot(
					nimbusData.getConf(), topologyId);
			try {
				// delete topologyId local dir
				PathUtils.rmr(master_stormdist_root);
			} catch (IOException e) {
				LOG.error("Failed to delete " + master_stormdist_root + ",", e);
			}
		}
	}

	/**
	 * get topology ids which need to be cleanup
	 * 
	 * @param clusterState
	 * @return
	 * @throws Exception
	 */
	private Set<String> get_cleanup_ids(StormClusterState clusterState,
			List<String> active_topologys) throws Exception {

		// get ZK /taskbeats/topology list and /taskerror/topology list
		List<String> heartbeat_ids = clusterState.heartbeat_storms();
		List<String> error_ids = clusterState.task_error_storms();

		String master_stormdist_root = StormConfig
				.masterStormdistRoot(nimbusData.getConf());
		// listdir /local-dir/nimbus/stormdist
		List<String> code_ids = PathUtils
				.read_dir_contents(master_stormdist_root);

		// Set<String> assigned_ids =
		// JStormUtils.listToSet(clusterState.active_storms());
		Set<String> to_cleanup_ids = new HashSet<String>();
		if (heartbeat_ids != null) {
			to_cleanup_ids.addAll(heartbeat_ids);
		}
		if (error_ids != null) {
			to_cleanup_ids.addAll(error_ids);
		}
		if (code_ids != null) {
			to_cleanup_ids.addAll(code_ids);
		}
		if (active_topologys != null) {
			to_cleanup_ids.removeAll(active_topologys);
		}
		return to_cleanup_ids;
	}

	/**
	 * start a topology: set active status of the topology
	 * 
	 * @param topologyName
	 * @param stormClusterState
	 * @param topologyId
	 * @throws Exception
	 */
	public void setTopologyStatus(TopologyAssignEvent event) throws Exception {
		StormClusterState stormClusterState = nimbusData.getStormClusterState();

		String topologyId = event.getTopologyId();
		String topologyName = event.getTopologyName();
		String group = event.getGroup();

		StormStatus status = new StormStatus(StatusType.active);
		if (event.getOldStatus() != null) {
			status = event.getOldStatus();
		}

		StormBase stormBase = stormClusterState.storm_base(topologyId, null);
		if (stormBase == null) {
			stormBase = new StormBase(topologyName,
					TimeUtils.current_time_secs(), status, group);
			stormClusterState.activate_storm(topologyId, stormBase);

		} else {

			stormClusterState.update_storm(topologyId, status);

			// here exist one hack operation
			// when monitor/rebalance/startup topologyName is null
			if (topologyName == null) {
				event.setTopologyName(stormBase.getStormName());
			}
		}

		LOG.info("Update " + topologyId + " " + status);

	}

	protected TopologyAssignContext prepareTopologyAssign(
			TopologyAssignEvent event) throws Exception {
		TopologyAssignContext ret = new TopologyAssignContext();

		String topologyId = event.getTopologyId();

		Map<Object, Object> conf = nimbusData.getConf();

		StormTopology rawTopology = StormConfig.read_nimbus_topology_code(conf,
				topologyId);
		ret.setRawTopology(rawTopology);

		Map stormConf = new HashMap();
		stormConf.putAll(conf);
		stormConf.putAll(StormConfig
				.read_nimbus_topology_conf(conf, topologyId));
		ret.setStormConf(stormConf);

		StormClusterState stormClusterState = nimbusData.getStormClusterState();

		// get all running supervisor, don't need callback to watch supervisor
		Map<String, SupervisorInfo> supInfos = Cluster.allSupervisorInfo(
				stormClusterState, null);
		if (supInfos.size() == 0) {
			throw new FailedAssignTopologyException(
					"Failed to make assignment " + topologyId
							+ ", due to no alive supervisor");
		}

		Map<Integer, String> taskToComponent = Cluster.topology_task_info(
				stormClusterState, topologyId);
		ret.setTaskToComponent(taskToComponent);

		// get taskids /ZK/tasks/topologyId
		Set<Integer> allTaskIds = taskToComponent.keySet();
		if (allTaskIds == null || allTaskIds.size() == 0) {
			String errMsg = "Failed to get all task ID list from /ZK-dir/tasks/"
					+ topologyId;
			LOG.warn(errMsg);
			throw new IOException(errMsg);
		}
		ret.setAllTaskIds(allTaskIds);

		Set<Integer> aliveTasks = new HashSet<Integer>();
		// unstoppedTasks are tasks which are alive on no supervisor's(dead)
		// machine
		Set<Integer> unstoppedTasks = new HashSet<Integer>();
		Set<Integer> deadTasks = new HashSet<Integer>();

		Assignment existingAssignment = stormClusterState.assignment_info(
				topologyId, null);
		if (existingAssignment != null) {
			aliveTasks = getAliveTasks(topologyId, allTaskIds);
			unstoppedTasks = getUnstoppedSlots(aliveTasks, supInfos,
					existingAssignment);

			deadTasks.addAll(allTaskIds);
			deadTasks.removeAll(aliveTasks);
		}

		ret.setDeadTaskIds(deadTasks);
		ret.setUnstoppedTaskIds(unstoppedTasks);

		// Step 2: get all slots resource, free slots/ alive slots/ unstopped
		// slots
		getFreeSlots(supInfos, stormClusterState);
		ret.setCluster(supInfos);

		if (existingAssignment == null) {
			ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_NEW);

			try {
				AssignmentBak lastAssignment = stormClusterState
					.assignment_bak(event.getTopologyName());
				if (lastAssignment != null) {

					ret.setOldAssignment(lastAssignment.getAssignment());
				}

			}catch(Exception e) {
				LOG.warn("Failed to get old Assignment,", e);
			}
		} else {
			ret.setOldAssignment(existingAssignment);
			if (event.isScratch()) {
				ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_REBALANCE);
			} else {
				ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_MONITOR);
			}
		}

		return ret;
	}

	private void checkGroupResource(Map conf,
			Map<Integer, ResourceAssignment> assignment, String topologyName)
			throws Exception {
		if (!nimbusData.isGroupMode())
			return;
		String group = ConfigExtension.getUserGroup(conf);
		if (group == null)
			throw new FailedAssignTopologyException(
					"It's group model, do you forget to set group name in your topology's conf? ^_^");
		Map<ThriftResourceType, Integer> groupResource = nimbusData
				.getGroupToResource().get(group);
		if (groupResource == null)
			throw new FailedAssignTopologyException("Your group name: " + group
					+ " is not valid");
		Map<ThriftResourceType, Integer> usedResource = nimbusData
				.getGroupToUsedResource().get(group);
		if (usedResource == null) {
			usedResource = new HashMap<ThriftResourceType, Integer>();
			nimbusData.getGroupToUsedResource().put(group, usedResource);
			usedResource.put(ThriftResourceType.CPU, 0);
			usedResource.put(ThriftResourceType.MEM, 0);
			usedResource.put(ThriftResourceType.DISK, 0);
			usedResource.put(ThriftResourceType.NET, 0);
		}
		Map<ThriftResourceType, Integer> topologyResource = NimbusUtils
				.getTopologyResource(null, assignment);
		for (Entry<ThriftResourceType, Integer> entry : topologyResource
				.entrySet()) {
			int used = usedResource.get(entry.getKey());
			int had = groupResource.get(entry.getKey());
			if ((used + entry.getValue()) > had) {
				StringBuilder failMSG = new StringBuilder();
				failMSG.append("Your group '").append(group)
						.append("' have not enough ")
						.append(entry.getKey().name()).append("\n")
						.append("Your group's ").append(entry.getKey())
						.append("POOL size is:").append(had).append(" ")
						.append("and had been used:").append(used).append(" ")
						.append("but your topology '").append(topologyName)
						.append("' need:").append(entry.getValue())
						.append("\n");
				throw new FailedAssignTopologyException(failMSG.toString());
			}
		}
		takeTopologyResource(group, topologyResource, usedResource,
				topologyName);
	}

	private void takeTopologyResource(String group,
			Map<ThriftResourceType, Integer> topologyResource,
			Map<ThriftResourceType, Integer> usedResource, String topologyName)
			throws Exception {
		for (Entry<ThriftResourceType, Integer> entry : topologyResource
				.entrySet()) {
			usedResource.put(entry.getKey(), usedResource.get(entry.getKey())
					+ entry.getValue());
		}
		Map<String, Map<ThriftResourceType, Integer>> topologys = nimbusData
				.getGroupToTopology().get(group);
		if (topologys == null) {
			topologys = new HashMap<String, Map<ThriftResourceType, Integer>>();
			nimbusData.getGroupToTopology().put(group, topologys);
		}
		topologys.put(topologyName, topologyResource);
	}

	private void upadateUsedResrouce(String topologyId,
			Map<Integer, ResourceAssignment> assignment) throws Exception {
		StormBase topology = nimbusData.getStormClusterState().storm_base(
				topologyId, null);
		String topologyName = topology.getStormName();
		String groupName = topology.getGroup();
		Map<ThriftResourceType, Integer> used = nimbusData
				.getGroupToUsedResource().get(groupName);
		Map<ThriftResourceType, Integer> topologyResource = NimbusUtils
				.getTopologyResource(null, assignment);
		NimbusUtils.releaseGroupResource(nimbusData, topologyName, groupName);
		takeTopologyResource(groupName, topologyResource, used, topologyName);
	}

	public void updateGroupResource(TopologyAssignContext context,
			TopologyAssignEvent event,
			Map<Integer, ResourceAssignment> taskAssignments) throws Exception {

		if (nimbusData.isGroupMode() == false) {
			return;
		}

		String topologyId = event.getTopologyId();
		nimbusData.getFlushGroupFileLock().lock();
		try {
			if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_NEW) {
				String topologyName = event.getTopologyName();
				checkGroupResource(context.getStormConf(), taskAssignments,
						topologyName);
				event.setGroup(ConfigExtension.getUserGroup(context
						.getStormConf()));
			} else {
				upadateUsedResrouce(topologyId, taskAssignments);
			}
		} finally {
			nimbusData.getFlushGroupFileLock().unlock();
		}

		return;
	}

	/**
	 * make assignments for a topology
	 * 
	 * The nimbus core function, this function has been totally rewrite
	 * 
	 * 
	 * @param nimbusData
	 *            NimbusData
	 * @param topologyId
	 *            String
	 * @param isScratch
	 *            Boolean: isScratch is false unless rebalancing the topology
	 * @throws Exception
	 */
	public Assignment mkAssignment(TopologyAssignEvent event) throws Exception {
		String topologyId = event.getTopologyId();

		LOG.info("Determining assignment for " + topologyId);

		TopologyAssignContext context = prepareTopologyAssign(event);

		Map<Integer, ResourceAssignment> taskAssignments = null;

		if (!StormConfig.local_mode(nimbusData.getConf())) {

			IToplogyScheduler scheduler = schedulers
					.get(DEFAULT_SCHEDULER_NAME);

			taskAssignments = scheduler.assignTasks(context);
			updateGroupResource(context, event, taskAssignments);

		} else {
			taskAssignments = mkLocalAssignment(context);
		}
		Assignment assignment = null;

		Map<String, String> nodeHost = getTopologyNodeHost(
				context.getCluster(), context.getOldAssignment(),
				taskAssignments);

		Map<Integer, Integer> startTimes = getTaskStartTimes(context,
				nimbusData, topologyId, context.getOldAssignment(),
				taskAssignments);

		String codeDir = StormConfig.masterStormdistRoot(nimbusData.getConf(),
				topologyId);

		assignment = new Assignment(codeDir, taskAssignments, nodeHost,
				startTimes);

		StormClusterState stormClusterState = nimbusData.getStormClusterState();

		stormClusterState.set_assignment(topologyId, assignment);

		// update task heartbeat's start time
		NimbusUtils.updateTaskHbStartTime(nimbusData, assignment, topologyId);

		LOG.info("Successfully make assignment for topology id " + topologyId
				+ ": " + assignment);

		return assignment;
	}

	private static Map<Integer, ResourceAssignment> mkLocalAssignment(
			TopologyAssignContext context) {
		Map<Integer, ResourceAssignment> result = new HashMap<Integer, ResourceAssignment>();
		Map<String, SupervisorInfo> cluster = context.getCluster();
		if (cluster.size() != 1)
			throw new RuntimeException();
		SupervisorInfo localSupervisor = null;
		String supervisorId = null;
		for (Entry<String, SupervisorInfo> entry : cluster.entrySet()) {
			supervisorId = entry.getKey();
			localSupervisor = entry.getValue();
		}
		int port = localSupervisor.getNetPool().alloc(null);
		for (Integer task : context.getAllTaskIds()) {
			ResourceAssignment resourceAssignment = new ResourceAssignment();
			resourceAssignment.setHostname(localSupervisor.getHostName());
			resourceAssignment.setSupervisorId(supervisorId);
			resourceAssignment.setPort(port);
			resourceAssignment.setCpuSlotNum(0);
			resourceAssignment.setMemSlotNum(0);
			result.put(task, resourceAssignment);
		}

		return result;
	}

	/**
	 * 
	 * @param existingAssignment
	 * @param taskWorkerSlot
	 * @return
	 * @throws Exception
	 */
	public static Map<Integer, Integer> getTaskStartTimes(
			TopologyAssignContext context, NimbusData nimbusData,
			String topologyId, Assignment existingAssignment,
			Map<Integer, ResourceAssignment> taskWorkerSlot) throws Exception {

		Map<Integer, Integer> startTimes = new TreeMap<Integer, Integer>();

		if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_NEW) {
			int nowSecs = TimeUtils.current_time_secs();
			for (Integer changedTaskId : taskWorkerSlot.keySet()) {
				startTimes.put(changedTaskId, nowSecs);
			}

			return startTimes;
		}

		Map<Integer, ResourceAssignment> oldTaskToWorkerSlot = new HashMap<Integer, ResourceAssignment>();

		if (existingAssignment != null) {
			Map<Integer, Integer> taskStartTimeSecs = existingAssignment
					.getTaskStartTimeSecs();
			if (taskStartTimeSecs != null) {
				startTimes.putAll(taskStartTimeSecs);
			}

			if (existingAssignment.getTaskToResource() != null) {
				oldTaskToWorkerSlot = existingAssignment.getTaskToResource();
			}
		}

		StormClusterState zkClusterState = nimbusData.getStormClusterState();
		Set<Integer> changeTaskIds = getChangeTaskIds(oldTaskToWorkerSlot,
				taskWorkerSlot);
		int nowSecs = TimeUtils.current_time_secs();
		for (Integer changedTaskId : changeTaskIds) {
			startTimes.put(changedTaskId, nowSecs);

			zkClusterState.remove_task_heartbeat(topologyId, changedTaskId);
		}

		LOG.info("Task assignment has been changed " + changeTaskIds);
		return startTimes;
	}

	public static Map<String, String> getTopologyNodeHost(
			Map<String, SupervisorInfo> supervisorMap,
			Assignment existingAssignment,
			Map<Integer, ResourceAssignment> taskWorkerSlot) {

		// the following is that remove unused node from allNodeHost
		Set<String> usedNodes = new HashSet<String>();
		for (Entry<Integer, ResourceAssignment> entry : taskWorkerSlot
				.entrySet()) {

			usedNodes.add(entry.getValue().getSupervisorId());
		}

		// map<supervisorId, hostname>
		Map<String, String> allNodeHost = new HashMap<String, String>();

		if (existingAssignment != null) {
			allNodeHost.putAll(existingAssignment.getNodeHost());
		}

		// get alive supervisorMap Map<supervisorId, hostname>
		Map<String, String> nodeHost = SupervisorInfo
				.getNodeHost(supervisorMap);
		if (nodeHost != null) {
			allNodeHost.putAll(nodeHost);
		}

		Map<String, String> ret = new HashMap<String, String>();

		for (String supervisorId : usedNodes) {
			if (allNodeHost.containsKey(supervisorId)) {
				ret.put(supervisorId, allNodeHost.get(supervisorId));
			} else {
				LOG.warn("Node " + supervisorId
						+ " doesn't in the supervisor list");
			}
		}

		return ret;
	}

	/**
	 * get all taskids which should be reassigned
	 * 
	 * @param taskToWorkerSlot
	 * @param newtaskToWorkerSlot
	 * @return Set<Integer> taskid which should reassigned
	 */
	public static Set<Integer> getChangeTaskIds(
			Map<Integer, ResourceAssignment> oldTaskToWorkerSlot,
			Map<Integer, ResourceAssignment> newTaskToWorkerSlot) {

		Map<WorkerSlot, List<Integer>> oldSlotAssigned = Assignment
				.getWorkerTasks(oldTaskToWorkerSlot);
		Map<WorkerSlot, List<Integer>> newSlotAssigned = Assignment
				.getWorkerTasks(newTaskToWorkerSlot);

		Set<Integer> rtn = new HashSet<Integer>();

		for (Entry<WorkerSlot, List<Integer>> entry : newSlotAssigned
				.entrySet()) {

			List<Integer> oldList = oldSlotAssigned.get(entry.getKey());
			List<Integer> newList = entry.getValue();
			if (oldList == null) {
				rtn.addAll(newList);
				continue;
			} else {

				if (oldList.equals(newList) == false) {
					rtn.addAll(newList);
					continue;
				}

				// The following is that oldList is equal newList
				boolean isSame = true;
				for (Integer taskId : newList) {
					ResourceAssignment oldResource = oldTaskToWorkerSlot
							.get(taskId);
					ResourceAssignment newResource = newTaskToWorkerSlot
							.get(taskId);

					if (oldResource == null) {
						// shouldn't occur
						isSame = false;
						break;
					} else if (oldResource.equals(newResource) == false) {
						isSame = false;
						break;
					}
				}

				if (isSame == false) {
					rtn.addAll(newList);
					continue;
				}
			}

		}
		return rtn;
	}

	/**
	 * sort slots, the purpose is to ensure that the tasks are assigned in
	 * balancing
	 * 
	 * @param allSlots
	 * @return List<WorkerSlot>
	 * 
	 */
	public static List<WorkerSlot> sortSlots(Set<WorkerSlot> allSlots,
			int needSlotNum) {

		Map<String, List<WorkerSlot>> nodeMap = new HashMap<String, List<WorkerSlot>>();

		// group by first
		for (WorkerSlot np : allSlots) {
			String node = np.getNodeId();

			List<WorkerSlot> list = nodeMap.get(node);
			if (list == null) {
				list = new ArrayList<WorkerSlot>();
				nodeMap.put(node, list);
			}

			list.add(np);

		}

		for (Entry<String, List<WorkerSlot>> entry : nodeMap.entrySet()) {
			List<WorkerSlot> ports = entry.getValue();

			Collections.sort(ports, new Comparator<WorkerSlot>() {

				@Override
				public int compare(WorkerSlot first, WorkerSlot second) {
					String firstNode = first.getNodeId();
					String secondNode = second.getNodeId();
					if (firstNode.equals(secondNode) == false) {
						return firstNode.compareTo(secondNode);
					} else {
						return first.getPort() - second.getPort();
					}

				}

			});
		}

		// interleave
		List<List<WorkerSlot>> splitup = new ArrayList<List<WorkerSlot>>(
				nodeMap.values());

		Collections.sort(splitup, new Comparator<List<WorkerSlot>>() {
			public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
				return o2.size() - o1.size();
			}
		});

		List<WorkerSlot> sortedFreeSlots = JStormUtils.interleave_all(splitup);

		if (sortedFreeSlots.size() <= needSlotNum) {
			return sortedFreeSlots;

		}

		// sortedFreeSlots > needSlotNum
		return sortedFreeSlots.subList(0, needSlotNum);
	}

	/**
	 * Get unstopped slots from alive task list
	 * 
	 * @param aliveAssigned
	 * @param supInfos
	 * @return
	 */
	public Set<Integer> getUnstoppedSlots(Set<Integer> aliveTasks,
			Map<String, SupervisorInfo> supInfos, Assignment existAssignment) {
		Set<Integer> ret = new HashSet<Integer>();

		Map<Integer, ResourceAssignment> oldAssignment = existAssignment
				.getTaskToResource();

		Set<String> aliveSupervisors = supInfos.keySet();

		for (Entry<Integer, ResourceAssignment> entry : oldAssignment
				.entrySet()) {
			Integer taskId = entry.getKey();
			ResourceAssignment resource = entry.getValue();
			if (aliveTasks.contains(taskId) == false) {
				// task is dead
				continue;
			}

			String oldTaskSupervisorId = resource.getSupervisorId();

			if (aliveSupervisors.contains(oldTaskSupervisorId) == false) {
				// supervisor is dead
				ret.add(taskId);
				continue;
			}

		}

		return ret;

	}

	/**
	 * Get free resources
	 * 
	 * @param supervisorInfos
	 * @param stormClusterState
	 * @throws Exception
	 */
	public static void getFreeSlots(
			Map<String, SupervisorInfo> supervisorInfos,
			StormClusterState stormClusterState) throws Exception {

		Map<String, Assignment> assignments = Cluster.get_all_assignment(
				stormClusterState, null);

		for (Entry<String, Assignment> entry : assignments.entrySet()) {
			String topologyId = entry.getKey();
			Assignment assignment = entry.getValue();

			Map<Integer, ResourceAssignment> taskAssignments = assignment
					.getTaskToResource();

			for (ResourceAssignment resource : taskAssignments.values()) {

				SupervisorInfo supervisorInfo = supervisorInfos.get(resource
						.getSupervisorId());
				if (supervisorInfo == null) {
					// the supervisor is dead
					continue;
				}

				supervisorInfo.getCpuPool().alloc(resource.getCpuSlotNum(),
						null);
				supervisorInfo.getMemPool().alloc(resource.getMemSlotNum(),
						null);
				supervisorInfo.getDiskPool()
						.alloc(resource.getDiskSlot(), null);
				supervisorInfo.getNetPool().alloc(resource.getPort(), null);
			}
		}

	}

	/**
	 * find all alived taskid
	 * 
	 * Does not assume that clocks are synchronized. Task heartbeat is only used
	 * so that nimbus knows when it's received a new heartbeat. All timing is
	 * done by nimbus and tracked through task-heartbeat-cache
	 * 
	 * 
	 * @param conf
	 * @param topologyId
	 * @param stormClusterState
	 * @param taskIds
	 * @param taskStartTimes
	 * @param taskHeartbeatsCache
	 *            --Map<topologyId, Map<taskid, Map<tkHbCacheTime, time>>>
	 * @return Set<Integer> : taskid
	 * @throws Exception
	 */
	public Set<Integer> getAliveTasks(String topologyId, Set<Integer> taskIds)
			throws Exception {

		Set<Integer> aliveTasks = new HashSet<Integer>();

		// taskIds is the list from ZK /ZK-DIR/tasks/topologyId
		for (int taskId : taskIds) {

			boolean isDead = NimbusUtils.isTaskDead(nimbusData, topologyId,
					taskId);
			if (isDead == false) {
				aliveTasks.add(taskId);
			}

		}

		return aliveTasks;
	}

	/**
	 * Backup the toplogy's Assignment to ZK
	 * 
	 * @@@ Question Do we need to do backup operation every time?
	 * 
	 * @param assignment
	 * @param event
	 */
	public void backupAssignment(Assignment assignment,
			TopologyAssignEvent event) {
		String topologyId = event.getTopologyId();
		String topologyName = event.getTopologyName();
		try {

			StormClusterState zkClusterState = nimbusData
					.getStormClusterState();
			// one little problem, get tasks twice when assign one topology
			HashMap<Integer, String> tasks = Cluster.topology_task_info(
					zkClusterState, topologyId);

			Map<String, List<Integer>> componentTasks = JStormUtils
					.reverse_map(tasks);

			for (Entry<String, List<Integer>> entry : componentTasks.entrySet()) {
				List<Integer> keys = entry.getValue();

				Collections.sort(keys);

			}

			AssignmentBak assignmentBak = new AssignmentBak(componentTasks,
					assignment);
			zkClusterState.backup_assignment(topologyName, assignmentBak);

		} catch (Exception e) {
			LOG.warn("Failed to backup " + topologyId + " assignment "
					+ assignment, e);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
