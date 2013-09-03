package com.alipay.dw.jstorm.daemon.nimbus;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.Cluster;
import com.alipay.dw.jstorm.cluster.StormBase;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.cluster.StormStatus;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.daemon.supervisor.SupervisorInfo;
import com.alipay.dw.jstorm.task.Assignment;
import com.alipay.dw.jstorm.utils.PathUtils;
import com.alipay.dw.jstorm.utils.TimeUtils;
import com.esotericsoftware.minlog.Log;

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
    
    @SuppressWarnings("hiding")
    class EventTimoutCb<String, TopologyAssignEvent> implements
            TimeCacheMap.ExpiredCallback<String, TopologyAssignEvent> {
        
        @Override
        public void expire(String topologyId, TopologyAssignEvent event) {
            
            TopologyAssign
                    .push((com.alipay.dw.jstorm.daemon.nimbus.TopologyAssignEvent) event);
        }
        
    }
    
    protected TimeCacheMap<String, TopologyAssignEvent> failedEvents = new TimeCacheMap<String, TopologyAssignEvent>(
                                                                             10,
                                                                             3,
                                                                             new EventTimoutCb<String, TopologyAssignEvent>());
    
    protected NimbusData                                nimbusData;
    
    public void init(NimbusData nimbusData) {
        this.nimbusData = nimbusData;
        new Thread(this).start();
    }
    
    public void cleanup() {
        runFlag = false;
    }
    
    protected static LinkedBlockingQueue<TopologyAssignEvent> queue = new LinkedBlockingQueue<TopologyAssignEvent>();
    
    public static void push(TopologyAssignEvent event) {
        queue.add(event);
    }
    
    boolean runFlag = false;
    
    public void run() {
        LOG.info("TopologyAssign thread has been started");
        runFlag = true;
        
        while (runFlag) {
            TopologyAssignEvent event = queue.poll();
            if (event == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    
                }
                continue;
            }
            
            boolean isSuccess = doTopologyAssignment(event);
            if (isSuccess == false) {
                //failedEvents.put(event.getTopologyId(), event);
            } else {
                //failedEvents.remove(event.getTopologyId());
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
     * Create/Update topology assignment
     * set topology status
     * 
     * @param event
     * @return
     */
    protected boolean doTopologyAssignment(TopologyAssignEvent event) {
        try {
            mkAssignment(event.getTopologyId(), event.isScratch());
            
            setTopologyStatus(event);
        } catch (Exception e) {
            LOG.error("Failed to assign topology " + event.getTopologyId(), e);
            return false;
        }
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
            
            Log.info("Cleaning up " + topologyId);
            
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
                Log.error("Failed to delete " + master_stormdist_root + ",", e);
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
     * @param stormId
     * @throws Exception
     */
    public void setTopologyStatus(TopologyAssignEvent event) throws Exception {
        StormClusterState stormClusterState = nimbusData.getStormClusterState();
        
        String topologyId = event.getTopologyId();
        String topologyName = event.getTopologyName();
        
        if (topologyName != null) {
            // it is a new assignment
            LOG.info("Activating " + topologyName + ": " + topologyId);
            
            StormStatus status = new StormStatus(StatusType.active);
            StormBase stormBase = new StormBase(topologyName,
                    TimeUtils.current_time_secs(), status);
            stormClusterState.activate_storm(topologyId, stormBase);
        } else if (event.getOldStatus() != null) {
            StormStatus status = event.getOldStatus();
            // the last status only two type : active/inactive
            
            if (status.getStatusType() != StatusType.inactive ) {
                // if it isn't inactive status, then set it as active
                status.setStatusType(StatusType.active);
            }

            stormClusterState.update_storm(topologyId, event.getOldStatus());
            LOG.info("Restore topology status " + event.getOldStatus());
        }
        
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
    public Assignment mkAssignment(String topologyId, boolean isScratch)
            throws Exception {
        
        LOG.info("Determining assignment for " + topologyId);
        
        Map<Object, Object> conf = nimbusData.getConf();
        
        StormClusterState stormClusterState = nimbusData.getStormClusterState();
        
        // @@@ In the old logic, nimbus register Supervisor watcher
        // But in my thought, nimbus don't need watch supervisor
        
        //RunnableCallback callback = new TransitionZkCallback(nimbusData, topologyId);
        RunnableCallback callback = null;
        
        // get all running supervisor
        Map<String, SupervisorInfo> supervisorMap = Cluster.allSupervisorInfo(
                stormClusterState, callback);
        if (supervisorMap.size() == 0) {
            throw new IOException("Failed to make assignment " + topologyId
                    + ", due to no alive supervisor");
        }
        
        // get old assignment
        // don't need set callback here
        Assignment existingAssignment = stormClusterState.assignment_info(
                topologyId, null);
        
        // generate task to NodePort map
        Map<Integer, NodePort> taskNodePort = assignTasks(topologyId,
                existingAssignment, stormClusterState, callback, supervisorMap,
                isScratch);
        
        Map<String, String> nodeHost = getTopologyNodeHost(supervisorMap,
                existingAssignment, taskNodePort);
        
        Map<Integer, Integer> startTimes = getTaskStartTimes(
                existingAssignment, taskNodePort);
        
        String codeDir = StormConfig.masterStormdistRoot(conf, topologyId);
        
        Assignment assignment = new Assignment(codeDir, taskNodePort, nodeHost,
                startTimes);
        
        stormClusterState.set_assignment(topologyId, assignment);
        
        // update task heartbeat's start time
        NimbusUtils.updateTaskHbStartTime(nimbusData, assignment, topologyId);
        
        LOG.info("Successfully make assignment for topology id " + topologyId
                + ": " + assignment);
        
        return assignment;
    }
    
    /**
     * 
     * @param existingAssignment
     * @param taskNodePort
     * @return
     */
    public static Map<Integer, Integer> getTaskStartTimes(
            Assignment existingAssignment, Map<Integer, NodePort> taskNodePort) {
        
        Map<Integer, Integer> startTimes = new HashMap<Integer, Integer>();
        
        Map<Integer, NodePort> oldTaskToNodePort = new HashMap<Integer, NodePort>();
        
        if (existingAssignment != null) {
            Map<Integer, Integer> taskStartTimeSecs = existingAssignment
                    .getTaskStartTimeSecs();
            if (taskStartTimeSecs != null) {
                startTimes.putAll(taskStartTimeSecs);
            }
            
            if (existingAssignment.getTaskToNodeport() != null) {
                oldTaskToNodePort = existingAssignment.getTaskToNodeport();
            }
        }
        
        Set<Integer> changeTaskIds = getChangeTaskIds(oldTaskToNodePort,
                taskNodePort);
        int nowSecs = TimeUtils.current_time_secs();
        for (Integer changedTaskId : changeTaskIds) {
            startTimes.put(changedTaskId, nowSecs);
        }
        
        return startTimes;
    }
    
    public static Map<String, String> getTopologyNodeHost(
            Map<String, SupervisorInfo> supervisorMap,
            Assignment existingAssignment, Map<Integer, NodePort> taskNodePort) {
        
        // the following is that remove unused node from allNodeHost
        Set<String> usedNodes = new HashSet<String>();
        for (Entry<Integer, NodePort> entry : taskNodePort.entrySet()) {
            
            usedNodes.add(entry.getValue().getNode());
        }
        
        // map<supervisorId, hostname>
        Map<String, String> allNodeHost = new HashMap<String, String>();
        
        if (existingAssignment != null) {
            allNodeHost = existingAssignment.getNodeHost();
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
     * Get alive assigned tasks
     * 
     * @param nimbusData
     * @param existingAssignment
     * @param stormClusterState
     * @param topologyId
     * @param isScratch
     * @return
     * @throws Exception
     */
    public Map<NodePort, List<Integer>> getAliveAssigned(
            Assignment existingAssignment, StormClusterState stormClusterState,
            Set<Integer> allTaskIds, String topologyId, boolean isScratch)
            throws Exception {
        Map<NodePort, List<Integer>> aliveAssigned = new HashMap<NodePort, List<Integer>>();
        
        if (existingAssignment == null) {
            // don't need get alive assigned tasks
            return aliveAssigned;
        }
        
        // get which task is alive through taskheartbeat
        Set<Integer> aliveIds = getAliveTasks(topologyId, allTaskIds);
        
        Map<NodePort, List<Integer>> existingAssigned = JStormUtils
                .reverse_map(existingAssignment.getTaskToNodeport());
        for (Entry<NodePort, List<Integer>> entry : existingAssigned.entrySet()) {
            
            Set<Integer> taskids = JStormUtils.listToSet(entry.getValue());
            
            if (aliveIds != null && aliveIds.containsAll(taskids)) {
                // only when all tasks are alive, it will be put into
                // aliveAssigned
                aliveAssigned.put(entry.getKey(), entry.getValue());
            }
        }
        
        return aliveAssigned;
    }
    
    /**
     * Assign tasks to <node,port>
     * 
     * The nimbus core function,
     * this function has been totally rewritten
     * 
     * get existing assignment (just the task->node+port map) -> default to {}
     * filter out ones which have a task timeout figure out available slots on
     * cluster. add to that the used valid slots to get total slots. figure out
     * how many tasks should be in each slot (e.g., 4, 4, 4, 5) only keep
     * existing slots that satisfy one of those slots. for rest, reassign them
     * across remaining slots edge case for slots with no task timeout but with
     * supervisor timeout... just treat these as valid slots that can be
     * reassigned to. worst comes to worse the task will timeout and won't
     * assign here next time around
     * 
     * 
     * Take care of the following case:
     * 1. rebalance the old topology
     * 2. some supervisor is unavailable,
     * but the tasks under the failure supervisor maybe works well
     * 3. some task share one worker
     * 4. worker number is less than available
     * 5. some workers has been killed
     * 6. during topology initialization, some workers can't startup
     * 
     * @param conf
     * @param stormId
     * @param existingAssignment
     * @param stormClusterState
     * @param callback
     * @param taskHeartbeatsCache
     * @param isScratch
     *            -- if do rebalance, isScratch is true, -- other case(new
     *            Topology/reassign), isScratch is false
     * @return Map<Integer, NodePort> <taskid, NodePort>
     * @throws Exception
     */
    public Map<Integer, NodePort> assignTasks(String topologyId,
            Assignment existingAssignment, StormClusterState stormClusterState,
            RunnableCallback callback, Map<String, SupervisorInfo> supInfos,
            boolean isScratch) throws Exception {
        
        // Step 1: get to know which slot is alive
        
        // get taskids /ZK/tasks/topologyId
        Set<Integer> allTaskIds = JStormUtils.listToSet(stormClusterState
                .task_ids(topologyId));
        if (allTaskIds == null || allTaskIds.size() == 0) {
            String errMsg = "Failed to get all task ID list from /ZK-dir/tasks/"
                    + topologyId;
            LOG.warn(errMsg);
            throw new IOException(errMsg);
        }
        
        // Step 2: get all slots resource,  free slots/ alive slots/ unstopped slots
        Set<NodePort> freeSlots = getFreeSlots(supInfos, stormClusterState);
        
        Map<NodePort, List<Integer>> aliveAssigned = getAliveAssigned(
                existingAssignment, stormClusterState, allTaskIds, topologyId,
                isScratch);
        
        Map<NodePort, List<Integer>> unstopped = getUnstoppedSlots(
                aliveAssigned, supInfos);
        
        return assignTasks(topologyId, existingAssignment, stormClusterState,
                supInfos, isScratch, allTaskIds, freeSlots, aliveAssigned,
                unstopped);
    }
    
    public Map<Integer, NodePort> assignTasks(String topologyId,
            Assignment existingAssignment, StormClusterState stormClusterState,
            Map<String, SupervisorInfo> supInfos, boolean isScratch,
            Set<Integer> allTaskIds, Set<NodePort> freeSlots,
            Map<NodePort, List<Integer>> aliveAssigned,
            Map<NodePort, List<Integer>> unstopped) throws Exception {
        
        // Step1 : get to know which slot need to be keep
        
        Map<?, ?> topology_conf = StormConfig.read_nimbus_topology_conf(
                nimbusData.getConf(), topologyId);
        
        // Step 1: check whether slot is enough for current assignment
        Integer workerNum = JStormUtils.parseInt(topology_conf
                .get(Config.TOPOLOGY_WORKERS));
        if (workerNum == null || workerNum <= 0) {
            // this won't happened, due to submit will do this check
            String errMsg = "There are no Config.TOPOLOGY_WORKERS in configuration of "
                    + topologyId;
            LOG.error(errMsg);
            throw new InvalidParameterException(errMsg);
        }
        
        Map<NodePort, List<Integer>> keepAssigned = new HashMap<NodePort, List<Integer>>();
        
        keepAssigned.putAll(unstopped);
        
        if (workerNum <= unstopped.size()) {
            // this won't happened, due to submit will do this check
            String errMsg = "Unstopped workers is bigger than the setting "
                    + topologyId;
            LOG.error(errMsg);
            throw new InvalidParameterException(errMsg);
        }
        
        if (isScratch == true) {
            // the left aliveAssigned slot won't be kept
        } else {
            Set<Integer> deadTasks = new HashSet<Integer>();
            
            deadTasks.addAll(allTaskIds);
            
            for (Entry<NodePort, List<Integer>> entry : aliveAssigned
                    .entrySet()) {
                List<Integer> aliveTasks = entry.getValue();
                
                deadTasks.removeAll(aliveTasks);
            }
            
            if (freeSlots.size() >= deadTasks.size()) {
                // free slots is bigger than dead tasks
                
                if ((workerNum - unstopped.size()) > aliveAssigned.size()) {
                    
                    // dead task will only used freeslot
                    keepAssigned.putAll(aliveAssigned);
                }
            }
        }
        
        Map<Integer, NodePort> stayAssignment = new HashMap<Integer, NodePort>();
        for (Entry<NodePort, List<Integer>> entry : keepAssigned.entrySet()) {
            NodePort np = entry.getKey();
            List<Integer> tasks = entry.getValue();
            
            for (Integer taskid : tasks) {
                stayAssignment.put(taskid, np);
            }
        }
        LOG.info("For " + topologyId + " keep slots " + stayAssignment);
        
        // Step 2 : get to know which slot will be used and sort them
        Set<NodePort> availableSlots = new TreeSet<NodePort>();
        availableSlots.addAll(freeSlots);
        availableSlots.addAll(aliveAssigned.keySet());
        availableSlots.removeAll(keepAssigned.keySet());
        
        // sort them, workerNum is definitely bigger than keepAssigned.size 
        List<NodePort> sortedAvailableSlots = sortSlots(availableSlots,
                (workerNum - keepAssigned.size()));
        
        TreeSet<Integer> needAssigneds = new TreeSet<Integer>();
        needAssigneds.addAll(allTaskIds);
        needAssigneds.removeAll(stayAssignment.keySet());
        
        Map<Integer, NodePort> newAssginment = new HashMap<Integer, NodePort>();
        
        int slotPos = 0;
        for (Integer newTask : needAssigneds) {
            newAssginment.put(newTask, sortedAvailableSlots.get(slotPos++));
            if (slotPos == sortedAvailableSlots.size()) {
                slotPos = 0;
            }
        }
        LOG.info("For " + topologyId + " new assign slots " + newAssginment);
        
        Map<Integer, NodePort> assignment = new HashMap<Integer, NodePort>();
        assignment.putAll(stayAssignment);
        assignment.putAll(newAssginment);
        
        return assignment;
        
    }
    
    /**
     * get all taskids which should be reassigned
     * 
     * @param taskToNodePort
     * @param newtaskToNodePort
     * @return Set<Integer> taskid which should reassigned
     */
    public static Set<Integer> getChangeTaskIds(
            Map<Integer, NodePort> oldTaskToNodePort,
            Map<Integer, NodePort> newTaskToNodePort) {
        
        Map<NodePort, List<Integer>> oldSlotAssigned = JStormUtils
                .reverse_map(oldTaskToNodePort);
        Map<NodePort, List<Integer>> newSlotAssigned = JStormUtils
                .reverse_map(newTaskToNodePort);
        
        Set<Integer> rtn = new HashSet<Integer>();
        
        for (Entry<NodePort, List<Integer>> entry : newSlotAssigned.entrySet()) {
            
            if (oldSlotAssigned.containsKey(entry.getKey()) == false) {
                List<Integer> lst = entry.getValue();
                rtn.addAll(lst);
            } else {
                List<Integer> oldList = oldSlotAssigned.get(entry.getKey());
                List<Integer> newList = entry.getValue();
                
                if (oldList == null) {
                    rtn.addAll(newList);
                    continue;
                } else if (newList == null) {
                    LOG.error("newSlotAssigned and newSlotAssign is null, "
                            + entry.getKey());
                    continue;
                }
                
                // The following is that oldList isn't null
                
                boolean isSame = true;
                if (oldList.containsAll(newList) == false) {
                    isSame = false;
                } else if (newList.containsAll(oldList) == false) {
                    isSame = false;
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
     * @return List<NodePort>
     * 
     */
    public static List<NodePort> sortSlots(Set<NodePort> allSlots,
            int needSlotNum) {
        
        Map<String, List<NodePort>> nodeMap = new HashMap<String, List<NodePort>>();
        
        // group by first
        for (NodePort np : allSlots) {
            String node = np.getNode();
            
            List<NodePort> list = nodeMap.get(node);
            if (list == null) {
                list = new ArrayList<NodePort>();
                nodeMap.put(node, list);
            }
            
            list.add(np);
            
        }
        
        for (Entry<String, List<NodePort>> entry : nodeMap.entrySet()) {
            List<NodePort> ports = entry.getValue();
            
            Collections.sort(ports);
        }
        
        // interleave
        List<List<NodePort>> splitup = new ArrayList<List<NodePort>>(
                nodeMap.values());
        
        Collections.sort(splitup, new Comparator<List<NodePort>> () {
            public int compare(List<NodePort> o1, List<NodePort> o2) {
                return o1.size() - o2.size();
            }
        });
        
        List<NodePort> sortedFreeSlots = JStormUtils.interleave_all(splitup);
        
        if (sortedFreeSlots.size() <= needSlotNum) {
            return sortedFreeSlots;
            
        }
        
        // sortedFreeSlots > needSlotNum
        return sortedFreeSlots.subList(0, needSlotNum);
    }
    
    /**
     * Get unstopped slots and remove them from aliveAssigned
     * 
     * @param aliveAssigned
     * @param supInfos
     * @return
     */
    public static Map<NodePort, List<Integer>> getUnstoppedSlots(
            Map<NodePort, List<Integer>> aliveAssigned,
            Map<String, SupervisorInfo> supInfos) {
        Map<NodePort, List<Integer>> unstopped = new HashMap<NodePort, List<Integer>>();
        
        Set<String> aliveSupervisors = supInfos.keySet();
        
        for (Entry<NodePort, List<Integer>> entry : aliveAssigned.entrySet()) {
            NodePort nodePort = entry.getKey();
            if (aliveSupervisors.contains(nodePort.getNode()) == false) {
                // the supervisor has been stopped
                unstopped.put(nodePort, entry.getValue());
                continue;
            }
            
        }
        
        return unstopped;
        
    }
    
    /**
     * find all ports which can be assigned
     * 
     * @param conf
     * @param stormClusterState
     * @param callback
     * @return Set<NodePort> : form supervisorid to ports
     * @throws Exception
     * 
     */
    public static Set<NodePort> getFreeSlots(
            Map<String, SupervisorInfo> supervisorInfos,
            StormClusterState stormClusterState) throws Exception {
        
        Set<NodePort> rtn = new HashSet<NodePort>();
        
        // <supervisorId, List<Interger workerPort>>
        Map<String, List<Integer>> allSlots = new HashMap<String, List<Integer>>();
        
        for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
            
            allSlots.put(entry.getKey(), entry.getValue().getWorkPorts());
            
        }
        
        // get all assigned slots <hostname, Set<port>>
        Map<String, Set<Integer>> assignedSlots = Cluster
                .assignedSlots(stormClusterState);
        
        // generate all avialable slots
        for (Entry<String, List<Integer>> entry : allSlots.entrySet()) {
            
            String supervisorid = entry.getKey();
            List<Integer> s = entry.getValue();
            
            Set<Integer> e = assignedSlots.get(supervisorid);
            if (e != null) {
                s.removeAll(e);
            }
            
            for (Iterator<Integer> iter = s.iterator(); iter.hasNext();) {
                NodePort nodeport = new NodePort(supervisorid, iter.next());
                rtn.add(nodeport);
            }
        }
        return rtn;
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
     *            --Map<stormid, Map<taskid, Map<tkHbCacheTime, time>>>
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
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
    }
    
}
