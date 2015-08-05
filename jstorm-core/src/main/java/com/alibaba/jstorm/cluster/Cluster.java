/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * storm operation ZK
 * 
 * @author yannian/longda/zhiyuan.ls
 * 
 */
public class Cluster {

    // TODO Need Migrate constants to ZkConstant

    private static Logger LOG = LoggerFactory.getLogger(Cluster.class);

    public static final String ZK_SEPERATOR = "/";

    public static final String ASSIGNMENTS_ROOT = "assignments";
    public static final String ASSIGNMENTS_BAK = "assignments_bak";
    public static final String TASKS_ROOT = "tasks";
    public static final String STORMS_ROOT = "topology";
    public static final String SUPERVISORS_ROOT = "supervisors";
    public static final String TASKBEATS_ROOT = "taskbeats";
    public static final String TASKERRORS_ROOT = "taskerrors";
    public static final String MASTER_ROOT = "nimbus_master";
    public static final String NIMBUS_SLAVE_ROOT = "nimbus_slave";
    public static final String METRIC_ROOT = "metrics";

    public static final String LAST_ERROR = "last_error";

    public static final String ASSIGNMENTS_SUBTREE;
    public static final String ASSIGNMENTS_BAK_SUBTREE;
    public static final String TASKS_SUBTREE;
    public static final String STORMS_SUBTREE;
    public static final String SUPERVISORS_SUBTREE;
    public static final String TASKBEATS_SUBTREE;
    public static final String TASKERRORS_SUBTREE;
    public static final String MASTER_SUBTREE;
    public static final String NIMBUS_SLAVE_SUBTREE;
    public static final String METRIC_SUBTREE;

    static {
        ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
        ASSIGNMENTS_BAK_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_BAK;
        TASKS_SUBTREE = ZK_SEPERATOR + TASKS_ROOT;
        STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
        SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
        TASKBEATS_SUBTREE = ZK_SEPERATOR + TASKBEATS_ROOT;
        TASKERRORS_SUBTREE = ZK_SEPERATOR + TASKERRORS_ROOT;
        MASTER_SUBTREE = ZK_SEPERATOR + MASTER_ROOT;
        NIMBUS_SLAVE_SUBTREE = ZK_SEPERATOR + NIMBUS_SLAVE_ROOT;
        METRIC_SUBTREE = ZK_SEPERATOR + METRIC_ROOT;
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

    public static String metric_path(String topology_id) {
        return METRIC_SUBTREE + ZK_SEPERATOR + topology_id;
    }

    public static String assignment_bak_path(String id) {
        return ASSIGNMENTS_BAK_SUBTREE + ZK_SEPERATOR + id;
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

    public static Map<Integer, TaskInfo> get_all_taskInfo(
            StormClusterState zkCluster, String topologyId) throws Exception {
        return  zkCluster.task_all_info(topologyId);
    }
    
    
    public static Map<Integer, String> get_all_task_component(
    		StormClusterState zkCluster, String topologyId, 
    		Map<Integer, TaskInfo> taskInfoMap) throws Exception {
        if (taskInfoMap == null) {
            taskInfoMap = get_all_taskInfo(zkCluster, topologyId);
        }
        
        if (taskInfoMap == null) {
            return null;
        }
        
        return Common.getTaskToComponent(taskInfoMap);
    }
    
    public static  Map<Integer, String> get_all_task_type(
    		StormClusterState zkCluster, String topologyId, 
    		Map<Integer, TaskInfo> taskInfoMap) throws Exception {
        if (taskInfoMap == null) {
            taskInfoMap = get_all_taskInfo(zkCluster, topologyId);
        }
        
        if (taskInfoMap == null) {
            return null;
        }
        
        return Common.getTaskToType(taskInfoMap);
    }

    public static Map<String, TaskHeartbeat> get_all_task_heartbeat(
            StormClusterState zkCluster, String topologyId) throws Exception {
        Map<String, TaskHeartbeat> ret = new HashMap<String, TaskHeartbeat>();

        List<String> taskList = zkCluster.heartbeat_tasks(topologyId);
        for (String taskId : taskList) {
            TaskHeartbeat hb =
                    zkCluster.task_heartbeat(topologyId,
                            Integer.valueOf(taskId));
            if (hb == null) {
                LOG.error("Failed to get hearbeat of " + topologyId + ":"
                        + taskId);
                continue;
            }

            ret.put(taskId, hb);
        }

        return ret;
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

                String zkTopologyName = Common.topologyIdToName(topology_id);
                if (storm_name.endsWith(zkTopologyName)) {
                    return topology_id;
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
    public static HashMap<String, StormBase> get_all_StormBase(
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
    public static Map<String, SupervisorInfo> get_all_SupervisorInfo(
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
                SupervisorInfo supervisorInfo =
                        stormClusterState.supervisor_info(supervisorId);
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

            Assignment assignment =
                    stormClusterState.assignment_info(topology_id, callback);

            if (assignment == null) {
                LOG.error("Failed to get Assignment of " + topology_id
                        + " from ZK");
                continue;
            }

            ret.put(topology_id, assignment);
        }

        return ret;
    }

    public static Map<String, String> get_all_nimbus_slave(
            StormClusterState stormClusterState) throws Exception {
        List<String> hosts = stormClusterState.get_nimbus_slaves();
        if (hosts == null || hosts.size() == 0) {
            return null;
        }

        Map<String, String> ret = new HashMap<String, String>();
        for (String host : hosts) {
            String time = stormClusterState.get_nimbus_slave_time(host);
            ret.put(host, time);
        }

        return ret;
    }

    public static String get_supervisor_hostname(
            StormClusterState stormClusterState, String supervisorId)
            throws Exception {
        SupervisorInfo supervisorInfo =
                stormClusterState.supervisor_info(supervisorId);
        if (supervisorInfo == null) {
            return null;
        } else {
            return supervisorInfo.getHostName();
        }
    }

    public static boolean is_topology_exist_error(
            StormClusterState stormClusterState, String topologyId)
            throws Exception {

        Map<Integer, String> lastErrMap =
                stormClusterState.topo_lastErr_time(topologyId);
        if (lastErrMap == null || lastErrMap.size() == 0) {
            return false;
        }

        int now = TimeUtils.current_time_secs();
        for (Entry<Integer, String> entry : lastErrMap.entrySet()) {
            Integer timeout = entry.getKey();
            Integer timestamp = Integer.valueOf(entry.getValue());

            if (now - timestamp <= timeout) {
                return true;
            }
        }

        return false;
    }
    
	public static Map<Integer, List<TaskError>> get_all_task_errors(
			StormClusterState stormClusterState, String topologyId) {
		Map<Integer, List<TaskError>> ret = new HashMap<Integer, List<TaskError>>();
		try {
			List<String> errorTasks = stormClusterState.task_error_ids(topologyId);
			if (errorTasks == null || errorTasks.size() == 0) {
				return ret;
			}

			for (String taskIdStr : errorTasks) {
				Integer taskId = -1;
				try {
					taskId = Integer.valueOf(taskIdStr);
				}catch(Exception e) {
					// skip last_error
					continue;
				}
				
				List<TaskError> taskErrorList = stormClusterState.task_errors(topologyId, taskId);
				ret.put(taskId, taskErrorList);
			}
			return ret;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return ret;
		}

	}

}
