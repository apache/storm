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

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormMonitor;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.DefaultTopologyScheduler;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
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
			
			clusterState.try_remove_storm(topologyId);
			//
			nimbusData.getTaskHeartbeatsCache().remove(topologyId);

			// get /nimbus/stormdist/topologyId
			String master_stormdist_root = StormConfig.masterStormdistRoot(
					nimbusData.getConf(), topologyId);
			try {
				// delete topologyId local dir
				PathUtils.rmr(master_stormdist_root);
			} catch (IOException e) {
				LOG.warn("Failed to delete " + master_stormdist_root + ",", e);
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


		List<String> task_ids = clusterState.task_storms();
		List<String> heartbeat_ids = clusterState.heartbeat_storms();
		List<String> error_ids = clusterState.task_error_storms();
		List<String> assignment_ids = clusterState.assignments(null);
		List<String> monitor_ids = clusterState.monitors();

		String master_stormdist_root = StormConfig
				.masterStormdistRoot(nimbusData.getConf());
		// listdir /local-dir/nimbus/stormdist
		List<String> code_ids = PathUtils
				.read_dir_contents(master_stormdist_root);

		// Set<String> assigned_ids =
		// JStormUtils.listToSet(clusterState.active_storms());
		Set<String> to_cleanup_ids = new HashSet<String>();
		
		if (task_ids != null) {
			to_cleanup_ids.addAll(task_ids);
		}
		
		if (heartbeat_ids != null) {
			to_cleanup_ids.addAll(heartbeat_ids);
		}
		
		if (error_ids != null) {
			to_cleanup_ids.addAll(error_ids);
		}
		
		if (assignment_ids != null) {
			to_cleanup_ids.addAll(assignment_ids);
		}
		
		if (monitor_ids != null) {
			to_cleanup_ids.addAll(monitor_ids);
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

		Map<Object, Object> nimbusConf = nimbusData.getConf();
		Map<Object, Object> topologyConf = StormConfig
				.read_nimbus_topology_conf(nimbusConf, topologyId);

		StormTopology rawTopology = StormConfig.read_nimbus_topology_code(
				nimbusConf, topologyId);
		ret.setRawTopology(rawTopology);

		Map stormConf = new HashMap();
		stormConf.putAll(nimbusConf);
		stormConf.putAll(topologyConf);
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
		Set<ResourceWorkerSlot> unstoppedWorkers = new HashSet<ResourceWorkerSlot>();

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
			} catch (Exception e) {
				LOG.warn("Fail to get old assignment", e);
			}
		} else {
			ret.setOldAssignment(existingAssignment);
			if (event.isScratch()) {
				ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_REBALANCE);
				unstoppedWorkers = getUnstoppedWorkers(unstoppedTasks,
						existingAssignment);
				ret.setUnstoppedWorkers(unstoppedWorkers);
			} else {
				ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_MONITOR);
				unstoppedWorkers = getUnstoppedWorkers(aliveTasks,
						existingAssignment);
				ret.setUnstoppedWorkers(unstoppedWorkers);
			}
		}

		return ret;
	}

	/**
	 * make assignments for a topology The nimbus core function, this function
	 * has been totally rewrite
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

		Set<ResourceWorkerSlot> assignments = null;

		if (!StormConfig.local_mode(nimbusData.getConf())) {

			IToplogyScheduler scheduler = schedulers
					.get(DEFAULT_SCHEDULER_NAME);

			assignments = scheduler.assignTasks(context);

		} else {
			assignments = mkLocalAssignment(context);
		}
		Assignment assignment = null;

		Map<String, String> nodeHost = getTopologyNodeHost(
				context.getCluster(), context.getOldAssignment(), assignments);

		Map<Integer, Integer> startTimes = getTaskStartTimes(context,
				nimbusData, topologyId, context.getOldAssignment(), assignments);

		String codeDir = StormConfig.masterStormdistRoot(nimbusData.getConf(),
				topologyId);

		assignment = new Assignment(codeDir, assignments, nodeHost, startTimes);

		StormClusterState stormClusterState = nimbusData.getStormClusterState();

		stormClusterState.set_assignment(topologyId, assignment);

		// update task heartbeat's start time
		NimbusUtils.updateTaskHbStartTime(nimbusData, assignment, topologyId);

		// Update metrics information in ZK when rebalance or reassignment
		// Only update metrics monitor status when creating topology
		if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE
				|| context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_MONITOR)
			NimbusUtils.updateMetricsInfo(nimbusData, topologyId, assignment);
		else
			metricsMonitor(event);

		LOG.info("Successfully make assignment for topology id " + topologyId
				+ ": " + assignment);

		return assignment;
	}

	private static Set<ResourceWorkerSlot> mkLocalAssignment(
			TopologyAssignContext context) {
		Set<ResourceWorkerSlot> result = new HashSet<ResourceWorkerSlot>();
		Map<String, SupervisorInfo> cluster = context.getCluster();
		if (cluster.size() != 1)
			throw new RuntimeException();
		SupervisorInfo localSupervisor = null;
		String supervisorId = null;
		for (Entry<String, SupervisorInfo> entry : cluster.entrySet()) {
			supervisorId = entry.getKey();
			localSupervisor = entry.getValue();
		}
		int port = localSupervisor.getWorkerPorts().iterator().next();
		ResourceWorkerSlot worker = new ResourceWorkerSlot(supervisorId, port);
		worker.setTasks(new HashSet<Integer>(context.getAllTaskIds()));
		worker.setHostname(localSupervisor.getHostName());
		result.add(worker);
		return result;
	}

	/**
	 * @param existingAssignment
	 * @param taskWorkerSlot
	 * @return
	 * @throws Exception
	 */
	public static Map<Integer, Integer> getTaskStartTimes(
			TopologyAssignContext context, NimbusData nimbusData,
			String topologyId, Assignment existingAssignment,
			Set<ResourceWorkerSlot> workers) throws Exception {

		Map<Integer, Integer> startTimes = new TreeMap<Integer, Integer>();

		if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_NEW) {
			int nowSecs = TimeUtils.current_time_secs();
			for (ResourceWorkerSlot worker : workers) {
				for (Integer changedTaskId : worker.getTasks()) {
					startTimes.put(changedTaskId, nowSecs);
				}
			}

			return startTimes;
		}

		Set<ResourceWorkerSlot> oldWorkers = new HashSet<ResourceWorkerSlot>();

		if (existingAssignment != null) {
			Map<Integer, Integer> taskStartTimeSecs = existingAssignment
					.getTaskStartTimeSecs();
			if (taskStartTimeSecs != null) {
				startTimes.putAll(taskStartTimeSecs);
			}

			if (existingAssignment.getWorkers() != null) {
				oldWorkers = existingAssignment.getWorkers();
			}
		}

		StormClusterState zkClusterState = nimbusData.getStormClusterState();
		Set<Integer> changeTaskIds = getChangeTaskIds(oldWorkers, workers);
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
			Assignment existingAssignment, Set<ResourceWorkerSlot> workers) {

		// the following is that remove unused node from allNodeHost
		Set<String> usedNodes = new HashSet<String>();
		for (ResourceWorkerSlot worker : workers) {

			usedNodes.add(worker.getNodeId());
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
			Set<ResourceWorkerSlot> oldWorkers, Set<ResourceWorkerSlot> workers) {

		Set<Integer> rtn = new HashSet<Integer>();
		for (ResourceWorkerSlot worker : workers) {
			if (!oldWorkers.contains(worker))
				rtn.addAll(worker.getTasks());
		}
		return rtn;
	}

	/**
	 * sort slots, the purpose is to ensure that the tasks are assigned in
	 * balancing
	 * 
	 * @param allSlots
	 * @return List<WorkerSlot>
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

		Set<ResourceWorkerSlot> oldWorkers = existAssignment.getWorkers();

		Set<String> aliveSupervisors = supInfos.keySet();

		for (ResourceWorkerSlot worker : oldWorkers) {
			for (Integer taskId : worker.getTasks()) {
				if (aliveTasks.contains(taskId) == false) {
					// task is dead
					continue;
				}

				String oldTaskSupervisorId = worker.getNodeId();

				if (aliveSupervisors.contains(oldTaskSupervisorId) == false) {
					// supervisor is dead
					ret.add(taskId);
					continue;
				}
			}
		}

		return ret;

	}

	private Set<ResourceWorkerSlot> getUnstoppedWorkers(
			Set<Integer> aliveTasks, Assignment existAssignment) {
		Set<ResourceWorkerSlot> ret = new HashSet<ResourceWorkerSlot>();
		for (ResourceWorkerSlot worker : existAssignment.getWorkers()) {
			boolean alive = true;
			for (Integer task : worker.getTasks()) {
				if (!aliveTasks.contains(task)) {
					alive = false;
					break;
				}
			}
			if (alive) {
				ret.add(worker);
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

			Set<ResourceWorkerSlot> workers = assignment.getWorkers();

			for (ResourceWorkerSlot worker : workers) {

				SupervisorInfo supervisorInfo = supervisorInfos.get(worker
						.getNodeId());
				if (supervisorInfo == null) {
					// the supervisor is dead
					continue;
				}
				supervisorInfo.getWorkerPorts().remove(worker.getPort());
			}
		}

	}

	/**
	 * find all alived taskid Does not assume that clocks are synchronized. Task
	 * heartbeat is only used so that nimbus knows when it's received a new
	 * heartbeat. All timing is done by nimbus and tracked through
	 * task-heartbeat-cache
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

	public void metricsMonitor(TopologyAssignEvent event) {
		String topologyId = event.getTopologyId();
		try {
			Map<Object, Object> conf = nimbusData.getConf();
			boolean isEnable = ConfigExtension.isEnablePerformanceMetrics(conf);
			StormClusterState zkClusterState = nimbusData
					.getStormClusterState();
			StormMonitor monitor = new StormMonitor(isEnable);
			zkClusterState.set_storm_monitor(topologyId, monitor);
		} catch (Exception e) {
			LOG.warn(
					"Failed to update metrics monitor status of " + topologyId,
					e);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
