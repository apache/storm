package com.alibaba.jstorm.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.TaskInfo;

/**
 * storm operation ZK
 * 
 * @author yannian/zhiyuan.ls
 * 
 */
public class Cluster {
	
	//TODO Need Migrate constants to ZkConstant
	
	private static Logger LOG = Logger.getLogger(Cluster.class);

	public static final String ZK_SEPERATOR = "/";

	public static final String ASSIGNMENTS_ROOT = "assignments";
	public static final String ASSIGNMENTS_BAK = "assignments_bak";
	public static final String TASKS_ROOT = "tasks";
	public static final String CODE_ROOT = "code";
	public static final String STORMS_ROOT = "topology";
	public static final String SUPERVISORS_ROOT = "supervisors";
	public static final String TASKBEATS_ROOT = "taskbeats";
	public static final String TASKERRORS_ROOT = "taskerrors";
	public static final String MASTER_ROOT = "nimbus_master";
	public static final String MONITOR_ROOT = "monitor";
	
	public static final String STATUS_DIR = "status";
	public static final String TASK_DIR = "task";
	public static final String WORKER_DIR = "worker";
	public static final String USER_DIR = "user";
	
	public static final String LAST_ERROR = "last_error";

	public static final String ASSIGNMENTS_SUBTREE;
	public static final String TASKS_SUBTREE;
	public static final String STORMS_SUBTREE;
	public static final String SUPERVISORS_SUBTREE;
	public static final String TASKBEATS_SUBTREE;
	public static final String TASKERRORS_SUBTREE;
	public static final String MASTER_SUBTREE;
	public static final String MONITOR_SUBTREE;

	static {
		ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
		TASKS_SUBTREE = ZK_SEPERATOR + TASKS_ROOT;
		STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
		SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
		TASKBEATS_SUBTREE = ZK_SEPERATOR + TASKBEATS_ROOT;
		TASKERRORS_SUBTREE = ZK_SEPERATOR + TASKERRORS_ROOT;
		MASTER_SUBTREE = ZK_SEPERATOR + MASTER_ROOT;
		MONITOR_SUBTREE = ZK_SEPERATOR + MONITOR_ROOT;
	}

	public static String supervisor_path(String id) {
		return SUPERVISORS_SUBTREE + ZK_SEPERATOR + id;
	}

	public static String assignment_path(String id) {
		return ASSIGNMENTS_SUBTREE + ZK_SEPERATOR + id;
	}

	public static String storm_path(String id) {
		return STORMS_SUBTREE + ZK_SEPERATOR + id;
	}

	public static String storm_task_root(String topology_id) {
		return TASKS_SUBTREE + ZK_SEPERATOR + topology_id;
	}

	public static String task_path(String topology_id, int task_id) {
		return storm_task_root(topology_id) + ZK_SEPERATOR + task_id;
	}

	public static String taskbeat_storm_root(String topology_id) {
		return TASKBEATS_SUBTREE + ZK_SEPERATOR + topology_id;
	}

	public static String taskbeat_path(String topology_id, int task_id) {
		return taskbeat_storm_root(topology_id) + ZK_SEPERATOR + task_id;
	}

	public static String taskerror_storm_root(String topology_id) {
		return TASKERRORS_SUBTREE + ZK_SEPERATOR + topology_id;
	}
	
	public static String lasterror_path(String topology_id) {
		return taskerror_storm_root(topology_id) + ZK_SEPERATOR + LAST_ERROR;
	}

	public static String taskerror_path(String topology_id, int task_id) {
		return taskerror_storm_root(topology_id) + ZK_SEPERATOR + task_id;
	}

	public static String monitor_path(String topology_id) {
		return MONITOR_SUBTREE + ZK_SEPERATOR + topology_id;
	}
	
	public static String monitor_status_path(String topology_id) {
		return monitor_path(topology_id) + ZK_SEPERATOR + STATUS_DIR;
	}
	
	public static String monitor_taskdir_path(String topology_id) {
		return monitor_path(topology_id) + ZK_SEPERATOR + TASK_DIR;
	}
	
	public static String monitor_workerdir_path(String topology_id) {
		return monitor_path(topology_id) + ZK_SEPERATOR + WORKER_DIR;
	}
	
	public static String monitor_userdir_path(String topology_id) {
		return monitor_path(topology_id) + ZK_SEPERATOR + USER_DIR;
	}

	public static String monitor_task_path(String topology_id, String task_id) {
		return monitor_taskdir_path(topology_id) + ZK_SEPERATOR + task_id;
	}
	
	public static String monitor_worker_path(String topology_id, String worker_id) {
		return monitor_workerdir_path(topology_id) + ZK_SEPERATOR + worker_id;
	}
	
	public static String monitor_user_path(String topology_id, String worker_id) {
		return monitor_userdir_path(topology_id) + ZK_SEPERATOR + worker_id;
	}
	
	public static Object maybe_deserialize(byte[] data) {
		if (data == null) {
			return null;
		}
		return Utils.deserialize(data, null);
	}

	@SuppressWarnings("rawtypes")
	public static StormClusterState mk_storm_cluster_state(
			Map cluster_state_spec) throws Exception {
		return new StormZkClusterState(cluster_state_spec);
	}

	public static StormClusterState mk_storm_cluster_state(
			ClusterState cluster_state_spec) throws Exception {
		return new StormZkClusterState(cluster_state_spec);
	}

	/**
	 * return Map<taskId, ComponentId>
	 * 
	 * @param zkCluster
	 * @param topology_id
	 * @return
	 * @throws Exception
	 */
	public static HashMap<Integer, String> topology_task_info(
			StormClusterState zkCluster, String topology_id) throws Exception {
		HashMap<Integer, String> rtn = new HashMap<Integer, String>();

		List<Integer> taks_ids = zkCluster.task_ids(topology_id);

		for (Integer task : taks_ids) {
			TaskInfo info = zkCluster.task_info(topology_id, task);
			if (info == null) {
				LOG.error("Failed to get TaskInfo of " + topology_id
						+ ",taskid:" + task);
				continue;
			}
			String componentId = info.getComponentId();
			rtn.put(task, componentId);
		}

		return rtn;
	}
	
	/**
	 * return Map<taskId, ComponentType>
	 * 
	 * @param zkCluster
	 * @param topology_id
	 * @return
	 * @throws Exception
	 */
	public static HashMap<Integer, String> topology_task_compType(
			StormClusterState zkCluster, String topology_id) throws Exception {
		HashMap<Integer, String> rtn = new HashMap<Integer, String>();

		List<Integer> taks_ids = zkCluster.task_ids(topology_id);

		for (Integer task : taks_ids) {
			TaskInfo info = zkCluster.task_info(topology_id, task);
			if (info == null) {
				LOG.error("Failed to get TaskInfo of " + topology_id
						+ ",taskid:" + task);
				continue;
			}
			String componentType = info.getComponentType();
			rtn.put(task, componentType);
		}

		return rtn;
	}

	/**
	 * if one topology's name equal the input storm_name, then return the
	 * topology id, otherwise return null
	 * 
	 * @param zkCluster
	 * @param storm_name
	 * @return
	 * @throws Exception
	 */
	public static String get_topology_id(StormClusterState zkCluster,
			String storm_name) throws Exception {
		List<String> active_storms = zkCluster.active_storms();
		String rtn = null;
		if (active_storms != null) {
			for (String topology_id : active_storms) {
				if (topology_id.indexOf(storm_name) < 0) {
					continue;
				}

				StormBase base = zkCluster.storm_base(topology_id, null);
				if (base != null && storm_name.equals(base.getStormName())) {
					rtn = topology_id;
					break;
				}
			}
		}
		return rtn;
	}

	/**
	 * get all topology's StormBase
	 * 
	 * @param zkCluster
	 * @return <topology_id, StormBase>
	 * @throws Exception
	 */
	public static HashMap<String, StormBase> topology_bases(
			StormClusterState zkCluster) throws Exception {
		return get_topology_id(zkCluster);
	}

	public static HashMap<String, StormBase> get_topology_id(
			StormClusterState zkCluster) throws Exception {
		HashMap<String, StormBase> rtn = new HashMap<String, StormBase>();
		List<String> active_storms = zkCluster.active_storms();
		if (active_storms != null) {
			for (String topology_id : active_storms) {
				StormBase base = zkCluster.storm_base(topology_id, null);
				if (base != null) {
					rtn.put(topology_id, base);
				}
			}
		}
		return rtn;
	}

	/**
	 * get all SupervisorInfo of storm cluster
	 * 
	 * @param stormClusterState
	 * @param callback
	 * @return Map<String, SupervisorInfo> String: supervisorId SupervisorInfo:
	 *         [time-secs hostname worker-ports uptime-secs]
	 * @throws Exception
	 */
	public static Map<String, SupervisorInfo> allSupervisorInfo(
			StormClusterState stormClusterState, RunnableCallback callback)
			throws Exception {

		Map<String, SupervisorInfo> rtn = new TreeMap<String, SupervisorInfo>();
		// get /ZK/supervisors
		List<String> supervisorIds = stormClusterState.supervisors(callback);
		if (supervisorIds != null) {
			for (Iterator<String> iter = supervisorIds.iterator(); iter
					.hasNext();) {

				String supervisorId = iter.next();
				// get /supervisors/supervisorid
				SupervisorInfo supervisorInfo = stormClusterState
						.supervisor_info(supervisorId);
				if (supervisorInfo == null) {
					LOG.warn("Failed to get SupervisorInfo of " + supervisorId);
				} else {

					rtn.put(supervisorId, supervisorInfo);
				}
			}
		} else {
			LOG.info("No alive supervisor");
		}

		return rtn;
	}

	public static Map<String, Assignment> get_all_assignment(
			StormClusterState stormClusterState, RunnableCallback callback)
			throws Exception {
		Map<String, Assignment> ret = new HashMap<String, Assignment>();

		// get /assignments {topology_id}
		List<String> assignments = stormClusterState.assignments(callback);
		if (assignments == null) {
			LOG.debug("No assignment of ZK");
			return ret;
		}

		for (String topology_id : assignments) {

			Assignment assignment = stormClusterState.assignment_info(
					topology_id, callback);

			if (assignment == null) {
				LOG.error("Failed to get Assignment of " + topology_id
						+ " from ZK");
				continue;
			}

			ret.put(topology_id, assignment);
		}

		return ret;
	}

}
