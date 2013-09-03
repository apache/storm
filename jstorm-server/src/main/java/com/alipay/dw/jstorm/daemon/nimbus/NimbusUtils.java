package com.alipay.dw.jstorm.daemon.nimbus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alipay.dw.jstorm.cluster.Cluster;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.task.Assignment;
import com.alipay.dw.jstorm.task.TkHbCacheTime;
import com.alipay.dw.jstorm.task.heartbeat.TaskHeartbeat;
import com.alipay.dw.jstorm.utils.PathUtils;
import com.alipay.dw.jstorm.utils.TimeUtils;

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
    
    @SuppressWarnings("rawtypes")
    public static Map normalizeConf(Map conf, Map stormConf,
            StormTopology topology) throws Exception {
        
        List componentSers = new ArrayList();
        Set<String> cids = ThriftTopologyUtils.getComponentIds(topology);
        for (Iterator it = cids.iterator(); it.hasNext();) {
            String componentId = (String) it.next();
            
            ComponentCommon common = ThriftTopologyUtils.getComponentCommon(
                    topology, componentId);
            String json = common.get_json_conf();
            if (json != null) {
                Map mtmp = (Map) JStormUtils.from_json(json);
                
                Object ltmp =  mtmp.get(Config.TOPOLOGY_KRYO_REGISTER);
                if (ltmp != null) {
                    LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) +
                            ", componentId:" + componentId +
                            ", TOPOLOGY_KRYO_REGISTER" + ltmp.getClass().getName());
                    
                    if (ltmp instanceof List) {
                        for (Object o : (List) ltmp) {
                            LOG.info("TOPOLOGY_KRYO_REGISTER:entry:" + o);
                            componentSers.add(o);
                        }
                    }else {
                        componentSers.add(ltmp);
                    }
                    
                }
            }
        }
        
        Map totalConf = new HashMap();
        totalConf.putAll(conf);
        totalConf.putAll(stormConf);
        
        Object o = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
        List baseSers = null;
        if (o != null) {
            LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) + 
                    ", TOPOLOGY_KRYO_REGISTER" + o.getClass().getName());
            baseSers = (List) o;
        }
        
        Map map = new TreeMap();
        map.putAll(mapifySerializations(baseSers));
        map.putAll(mapifySerializations(componentSers));
        
        
        Map rtn = new HashMap();
        rtn.putAll(stormConf);
        rtn.put(Config.TOPOLOGY_KRYO_REGISTER, map);
        rtn.put(Config.TOPOLOGY_ACKERS, totalConf.get(Config.TOPOLOGY_ACKERS));
        return rtn;
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
            //zk has been updated the report time
            taskHB.setNimbusTime(nowSecs);
            taskHB.setTaskReportedTime(zkReportTime);
            
            LOG.debug(idStr + ",nimbusTime " + nowSecs + 
                    ",zkReport:" + zkReportTime + ",report:" + reportTime);
            return false;
        }
        
        // the following is (zkReportTime == reportTime)
        int taskHBTimeout = JStormUtils.parseInt(data.getConf().get(
                Config.NIMBUS_TASK_TIMEOUT_SECS));
        
        if (nowSecs - nimbusTime > taskHBTimeout) {
            // task is dead
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
        String topologyId = Cluster.get_storm_id(stormClusterState,
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
}
