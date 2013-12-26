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
 * @author yannian
 * 
 */
public class Cluster {
    private static Logger      LOG              = Logger.getLogger(Cluster.class);
    
    public static final String ZK_SEPERATOR     = "/";
    
    public static final String ASSIGNMENTS_ROOT = "assignments";
    public static final String ASSIGNMENTS_BAK  = "assignments_bak";
    public static final String TASKS_ROOT       = "tasks";
    public static final String CODE_ROOT        = "code";
    // change STORMS_ROOT from "storms" to "topology"
    public static final String STORMS_ROOT      = "topology";
    public static final String SUPERVISORS_ROOT = "supervisors";
    public static final String TASKBEATS_ROOT   = "taskbeats";
    public static final String TASKERRORS_ROOT  = "taskerrors";
    public static final String MASTER_ROOT      = "master";
    public static final String MASTER_LOCK_ROOT = "masterlock";
    
    
    public static final String ASSIGNMENTS_SUBTREE;
    public static final String ASSIGNMENTS_BAK_SUBTREE;
    public static final String TASKS_SUBTREE;
    public static final String STORMS_SUBTREE;
    public static final String SUPERVISORS_SUBTREE;
    public static final String TASKBEATS_SUBTREE;
    public static final String TASKERRORS_SUBTREE;
    public static final String MASTER_SUBTREE;
    public static final String MASTER_LOCK_SUBTREE;
    
    static {
        ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
        ASSIGNMENTS_BAK_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_BAK;
        TASKS_SUBTREE = ZK_SEPERATOR + TASKS_ROOT;
        STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
        SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
        TASKBEATS_SUBTREE = ZK_SEPERATOR + TASKBEATS_ROOT;
        TASKERRORS_SUBTREE = ZK_SEPERATOR + TASKERRORS_ROOT;
        MASTER_SUBTREE = ZK_SEPERATOR + MASTER_ROOT;
        MASTER_LOCK_SUBTREE = ZK_SEPERATOR + MASTER_LOCK_ROOT;
    }
    
    public static String supervisor_path(String id) {
        return SUPERVISORS_SUBTREE + ZK_SEPERATOR + id;
    }
    
    public static String assignment_path(String id) {
        return ASSIGNMENTS_SUBTREE + ZK_SEPERATOR + id;
    }
    
    public static String assignment_bak_path(String id) {
        return ASSIGNMENTS_BAK_SUBTREE + ZK_SEPERATOR + id;
    }
    
    public static String storm_path(String id) {
        return STORMS_SUBTREE + ZK_SEPERATOR + id;
    }
    
    public static String storm_task_root(String storm_id) {
        return TASKS_SUBTREE + ZK_SEPERATOR + storm_id;
    }
    
    public static String task_path(String storm_id, int task_id) {
        return storm_task_root(storm_id) + ZK_SEPERATOR + task_id;
    }
    
    public static String taskbeat_storm_root(String storm_id) {
        return TASKBEATS_SUBTREE + ZK_SEPERATOR + storm_id;
    }
    
    public static String taskbeat_path(String storm_id, int task_id) {
        return taskbeat_storm_root(storm_id) + ZK_SEPERATOR + task_id;
    }
    
    public static String taskerror_storm_root(String storm_id) {
        return TASKERRORS_SUBTREE + ZK_SEPERATOR + storm_id;
    }
    
    public static String taskerror_path(String storm_id, int task_id) {
        return taskerror_storm_root(storm_id) + ZK_SEPERATOR + task_id;
    }
    
    public static Object maybe_deserialize(byte[] data) {
        if (data == null) {
            return null;
        }
        return Utils.deserialize(data);
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
    
    @SuppressWarnings("rawtypes")
    public static ClusterState mk_distributed_cluster_state(Map _conf)
            throws Exception {
        return new DistributedClusterState(_conf);
    }
    
    /**
     * return Map<taskId, ComponentId>
     * 
     * @param zkCluster
     * @param topologyid
     * @return
     * @throws Exception
     */
    public static HashMap<Integer, String> topology_task_info(
            StormClusterState zkCluster, String topologyid) throws Exception {
        HashMap<Integer, String> rtn = new HashMap<Integer, String>();
        
        List<Integer> taks_ids = zkCluster.task_ids(topologyid);
        
        for (Integer task : taks_ids) {
            TaskInfo info = zkCluster.task_info(topologyid, task);
            if (info == null) {
                LOG.error("Failed to get TaskInfo of " + topologyid
                        + ",taskid:" + task);
                continue;
            }
            String componentId = info.getComponentId();
            rtn.put(task, componentId);
        }
        
        return rtn;
    }
    
    /**
     * if one topology's name equal the input storm_name,
     * then return the topology id, otherwise return null
     * 
     * @param zkCluster
     * @param storm_name
     * @return
     * @throws Exception
     */
    public static String get_storm_id(StormClusterState zkCluster,
            String storm_name) throws Exception {
        List<String> active_storms = zkCluster.active_storms();
        String rtn = null;
        if (active_storms != null) {
            for (String storm_id : active_storms) {
                if (storm_id.indexOf(storm_name) < 0) {
                    continue;
                }
                
                StormBase base = zkCluster.storm_base(storm_id, null);
                if (base != null && storm_name.equals(base.getStormName())) {
                    rtn = storm_id;
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
     * @return <topologyId, StormBase>
     * @throws Exception
     */
    public static HashMap<String, StormBase> topology_bases(
            StormClusterState zkCluster) throws Exception {
        return get_storm_id(zkCluster);
    }
    
    public static HashMap<String, StormBase> get_storm_id(
            StormClusterState zkCluster) throws Exception {
        HashMap<String, StormBase> rtn = new HashMap<String, StormBase>();
        List<String> active_storms = zkCluster.active_storms();
        if (active_storms != null) {
            for (String storm_id : active_storms) {
                StormBase base = zkCluster.storm_base(storm_id, null);
                if (base != null) {
                    rtn.put(storm_id, base);
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
        
        // get /assignments {topologyid}
        List<String> assignments = stormClusterState.assignments(callback);
        if (assignments == null) {
            LOG.debug("No assignment of ZK");
            return ret;
        }
        
        for (String topologyid : assignments) {
            
            Assignment assignment = stormClusterState.assignment_info(
                    topologyid, callback);
            
            if (assignment == null) {
                LOG.error("Failed to get Assignment of " + topologyid
                        + " from ZK");
                continue;
            }
            
            ret.put(topologyid, assignment);
        }
        
        return ret;
    }
    
    
}
