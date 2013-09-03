package com.alipay.dw.jstorm.daemon.nimbus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.TaskStats;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alipay.dw.jstorm.cluster.Cluster;
import com.alipay.dw.jstorm.cluster.Common;
import com.alipay.dw.jstorm.cluster.DaemonCommon;
import com.alipay.dw.jstorm.cluster.StormBase;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.daemon.supervisor.SupervisorInfo;
import com.alipay.dw.jstorm.stats.CommonStatsData;
import com.alipay.dw.jstorm.task.Assignment;
import com.alipay.dw.jstorm.task.TaskInfo;
import com.alipay.dw.jstorm.task.error.TaskError;
import com.alipay.dw.jstorm.task.heartbeat.TaskHeartbeat;
import com.alipay.dw.jstorm.utils.Thrift;
import com.alipay.dw.jstorm.utils.TimeUtils;

/**
 * Thrift callback, all commands handling entrance
 * 
 * @author version 1: lixin, version 2:Longda
 * 
 */
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
    private final static Logger LOG        = Logger.getLogger(ServiceHandler.class);
    
    public final static int     THREAD_NUM = 64;
    
    private NimbusData          data;
    
    private Map<Object, Object> conf;
    
    public ServiceHandler(NimbusData data) {
        this.data = data;
        conf = data.getConf();
    }
    
    /**
     * Submit one Topology
     * 
     * @param topologyname
     *            String: topology name
     * @param uploadedJarLocation
     *            String: already uploaded jar path
     * @param jsonConf
     *            String: jsonConf serialize all toplogy configuration to Json
     * @param topology
     *            StormTopology: topology Object
     */
    @SuppressWarnings("unchecked")
    @Override
    public void submitTopology(String topologyname, String uploadedJarLocation,
            String jsonConf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException, TException {
        LOG.info("Receive " + topologyname + ", uploadedJarLocation:"
                + uploadedJarLocation);
        try {
            checkTopologyActive(data, topologyname, false);
        } catch (AlreadyAliveException e) {
            LOG.info(topologyname + " is already exist ");
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to check whether topology is alive or not", e);
            throw new TException(e);
        }
        
        int counter = data.getSubmittedCount().incrementAndGet();
        String topologyId = topologyname + "-" + counter + "-"
                + TimeUtils.current_time_secs();
        
        Map<Object, Object> serializedConf = (Map<Object, Object>) JStormUtils
                .from_json(jsonConf);
        if (serializedConf == null) {
            LOG.warn("Failed to serialized Configuration");
            throw new InvalidTopologyException(
                    "Failed to serilaze topology configuration");
        }
        
        serializedConf.put(Config.STORM_ID, topologyId);
        
        Map<Object, Object> stormConf;
        try {
            stormConf = NimbusUtils.normalizeConf(conf, serializedConf,
                    topology);
        } catch (Exception e1) {
            String errMsg = JStormUtils.getErrorInfo(
                    "Failed to commit topologystormId: " + topologyId
                            + " uploadedJarLocation: " + uploadedJarLocation
                            + ", due to failed to normalize configuration", e1);
            LOG.info(errMsg);
            throw new TException(errMsg);
        }
        
        Map<Object, Object> totalStormConf = new HashMap<Object, Object>(conf);
        totalStormConf.putAll(stormConf);
        
        StormTopology newtopology = new StormTopology(topology);
        
        // this validates the structure of the topology
        Common.validate_basic(newtopology, totalStormConf, topologyId);
        // don't need generate real topology, so skip Common.system_topology
        // Common.system_topology(totalStormConf, newtopology);
        
        try {
            
            StormClusterState stormClusterState = data.getStormClusterState();
            
            // create /local-dir/nimbus/topologyId/xxxx files
            setupStormCode(conf, topologyId, uploadedJarLocation, stormConf,
                    newtopology);
            
            // generate TaskInfo for every bolt or spout in ZK
            // /ZK/tasks/topoologyId/xxx
            setupZkTaskInfo(conf, topologyId, stormClusterState);
            
            // make assignments for a topology
            TopologyAssignEvent assignEvent = new TopologyAssignEvent();
            assignEvent.setTopologyId(topologyId);
            assignEvent.setScratch(false);
            assignEvent.setTopologyName(topologyname);
            
            TopologyAssign.push(assignEvent);
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            
            String errMsg = JStormUtils.getErrorInfo(
                    "Fail to sumbit topology, stormId: " + topologyId
                            + " uploadedJarLocation: " + uploadedJarLocation
                            + ", failed mkAssignments.", e);
            LOG.info(errMsg);
            throw new TException(errMsg);
        } catch (Exception e) {
            String errMsg = JStormUtils.getErrorInfo(
                    "Failed to commit topologystormId: " + topologyId
                            + " uploadedJarLocation: " + uploadedJarLocation
                            + ", failed mkAssignments.", e);
            LOG.info(errMsg);
            throw new TException(errMsg);
        }
        
        LOG.info("Received topology submission for " + topologyname
                + " with conf " + serializedConf);
        
    }
    
    /**
     * kill topology
     * 
     * @param topologyname
     *            String topology name
     */
    @Override
    public void killTopology(String name) throws NotAliveException, TException {
        killTopologyWithOpts(name, new KillOptions());
        
    }
    
    @Override
    public void killTopologyWithOpts(String topologyName, KillOptions options)
            throws NotAliveException, TException {
        try {
            
            checkTopologyActive(data, topologyName, true);
            Integer wait_amt = null;
            if (options.is_set_wait_secs()) {
                wait_amt = options.get_wait_secs();
            }
            
            NimbusUtils.transitionName(data, topologyName, true,
                    StatusType.kill, wait_amt);
        } catch (NotAliveException e) {
            String errMsg = "KillTopology Error, no this topology "
                    + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to kill topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }
        
    }
    
    /**
     * set topology status as active
     * 
     * @param topologyname
     * 
     */
    @Override
    public void activate(String topologyName) throws NotAliveException,
            TException {
        try {
            NimbusUtils.transitionName(data, topologyName, true,
                    StatusType.activate);
        } catch (NotAliveException e) {
            String errMsg = "Activate Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to active topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }
        
    }
    
    /**
     * set topology stauts as deactive
     * 
     * @param topologyname
     * 
     */
    @Override
    public void deactivate(String topologyName) throws NotAliveException,
            TException {
        
        try {
            NimbusUtils.transitionName(data, topologyName, true,
                    StatusType.inactivate);
        } catch (NotAliveException e) {
            String errMsg = "Deactivate Error, no this topology "
                    + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to deactivate topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }
        
    }
    
    /**
     * rebalance one topology
     * 
     * @@@ rebalance options hasn't implements
     * 
     *     It is used to let workers wait several seconds to finish jobs
     * 
     * @param topologyname
     *            String
     * @param options
     *            RebalanceOptions
     */
    @Override
    public void rebalance(String topologyName, RebalanceOptions options)
            throws NotAliveException, TException {
        
        try {
            NimbusUtils.transitionName(data, topologyName, true,
                    StatusType.rebalance);
        } catch (NotAliveException e) {
            String errMsg = "Rebalance Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to rebalance topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }
        
    }
    
    /**
     * prepare to uploading topology jar, return the file location
     * 
     * @throws
     */
    @Override
    public String beginFileUpload() throws TException {
        String fileLoc = null;
        try {
            fileLoc = StormConfig.masterInbox(conf) + "/stormjar-"
                    + UUID.randomUUID() + ".jar";
            
            data.getUploaders().put(fileLoc,
                    Channels.newChannel(new FileOutputStream(fileLoc)));
            LOG.info("Uploading file from client to " + fileLoc);
        } catch (FileNotFoundException e) {
            LOG.error(" file not found " + fileLoc);
            throw new TException(e);
        } catch (IOException e) {
            LOG.error(" IOException  " + fileLoc, e);
            throw new TException(e);
        }
        return fileLoc;
    }
    
    /**
     * uploading topology jar data
     */
    @Override
    public void uploadChunk(String location, ByteBuffer chunk)
            throws TException {
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException(
                    "File for that location does not exist (or timed out) "
                            + location);
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.write(chunk);
                uploaders.put(location, channel);
            } else {
                throw new TException("Object isn't WritableByteChannel for "
                        + location);
            }
        } catch (IOException e) {
            String errMsg = " WritableByteChannel write filed when uploadChunk "
                    + location;
            LOG.error(errMsg);
            throw new TException(e);
        }
        
    }
    
    @Override
    public void finishFileUpload(String location) throws TException {
        
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException(
                    "File for that location does not exist (or timed out)");
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.close();
                uploaders.remove(location);
                LOG.info("Finished uploading file from client: " + location);
            } else {
                throw new TException("Object isn't WritableByteChannel for "
                        + location);
            }
        } catch (IOException e) {
            LOG.error(" WritableByteChannel close failed when finishFileUpload "
                    + location);
        }
        
    }
    
    @Override
    public String beginFileDownload(String file) throws TException {
        BufferFileInputStream is = null;
        String id = null;
        try {
            is = new BufferFileInputStream(file);
            id = UUID.randomUUID().toString();
            data.getDownloaders().put(id, is);
        } catch (FileNotFoundException e) {
            LOG.error(e + "file:" + file + " not found");
            throw new TException(e);
        }
        
        return id;
    }
    
    @Override
    public ByteBuffer downloadChunk(String id) throws TException {
        TimeCacheMap<Object, Object> downloaders = data.getDownloaders();
        Object obj = downloaders.get(id);
        if (obj == null) {
            throw new TException("Could not find input stream for that id");
        }
        byte[] ret = null;
        try {
            if (obj instanceof BufferFileInputStream) {
                BufferFileInputStream is = (BufferFileInputStream) obj;
                ret = is.read();
                if (ret != null) {
                    downloaders.put(id, (BufferFileInputStream) is);
                }
            } else {
                throw new TException("Object isn't BufferFileInputStream for "
                        + id);
            }
        } catch (IOException e) {
            LOG.error(e
                    + "BufferFileInputStream read failed when downloadChunk ");
            throw new TException(e);
        }
        
        return ByteBuffer.wrap(ret);
    }
    
    /**
     * get cluster's summary, it will contain SupervisorSummary and
     * TopologySummary
     * 
     * @return ClusterSummary
     */
    @Override
    public ClusterSummary getClusterInfo() throws TException {
        
        try {
            
            StormClusterState stormClusterState = data.getStormClusterState();
            
            Map<String, Assignment> assignments = new HashMap<String, Assignment>();
            
            // get nimbus running time
            int uptime = data.uptime();
            
            // get TopologySummary
            List<TopologySummary> topologySummaries = new ArrayList<TopologySummary>();
            
            // get all active topology's StormBase
            Map<String, StormBase> bases = Cluster
                    .topology_bases(stormClusterState);
            for (Entry<String, StormBase> entry : bases.entrySet()) {
                
                String stormId = entry.getKey();
                StormBase base = entry.getValue();
                
                Assignment assignment = stormClusterState.assignment_info(
                        stormId, null);
                if (assignment == null) {
                    LOG.error("Failed to get assignment of " + stormId);
                    continue;
                }
                assignments.put(stormId, assignment);
                
                HashSet<NodePort> workers = new HashSet<NodePort>();
                Collection<NodePort> entryColl = assignment.getTaskToNodeport()
                        .values();
                workers.addAll(entryColl);
                
                TopologySummary topology = new TopologySummary(stormId,
                        base.getStormName(), assignment.getTaskToNodeport()
                                .size(), workers.size(),
                        TimeUtils.time_delta(base.getLanchTimeSecs()),
                        extractStatusStr(base));
                
                topologySummaries.add(topology);
                
            }
            
            // assigned slots <supervisorId, Set<port>>
            Map<String, Set<Integer>> assigned = Cluster.assignedSlots(
                    stormClusterState, assignments);
            
            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster
                    .allSupervisorInfo(stormClusterState, null);
            
            // generate SupervisorSummaries
            List<SupervisorSummary> supervisorSummaries = new ArrayList<SupervisorSummary>();
            
            for (Entry<String, SupervisorInfo> entry : supervisorInfos
                    .entrySet()) {
                
                String supervisorId = entry.getKey();
                SupervisorInfo info = entry.getValue();
                
                int num_workers = 0;
                List<Integer> ports = info.getWorkPorts();
                if (ports != null) {
                    num_workers = ports.size();
                }
                
                int num_used_workers = 0;
                if (assigned != null && assigned.get(supervisorId) != null) {
                    num_used_workers = assigned.get(supervisorId).size();
                }
                
                supervisorSummaries.add(new SupervisorSummary(info
                        .getHostName(), info.getUptimeSecs(), num_workers,
                        num_used_workers));
            }
            
            return new ClusterSummary(supervisorSummaries, uptime,
                    topologySummaries);
            
        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        }
    }
    
    @Override
    public SupervisorWorkers getSupervisorWorkers(String host)
            throws NotAliveException, TException {
        try {
            
            StormClusterState stormClusterState = data.getStormClusterState();
            
            SupervisorSummary supervisorSummary = null;
            String supervisorId = null;
            
            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster
                    .allSupervisorInfo(stormClusterState, null);
            
            for (Entry<String, SupervisorInfo> entry : supervisorInfos
                    .entrySet()) {
                
                SupervisorInfo info = entry.getValue();
                
                if (info.getHostName().equals(host) == false) {
                    continue;
                }
                
                int num_workers = 0;
                List<Integer> ports = info.getWorkPorts();
                if (ports != null) {
                    num_workers = ports.size();
                }
                
                // set num_used_workers later
                int num_used_workers = 0;
                
                supervisorSummary = new SupervisorSummary(info.getHostName(),
                        info.getUptimeSecs(), num_workers, num_used_workers);
                supervisorId = entry.getKey();
            }
            
            if (supervisorSummary == null) {
                LOG.warn("No " + host + " supervisor");
                throw new NotAliveException("No " + host + " supervisor");
            }
            
            List<WorkerSummary> workers = new ArrayList<WorkerSummary>();
            
            // get all active topology's StormBase
            Map<String, StormBase> bases = Cluster
                    .topology_bases(stormClusterState);
            for (Entry<String, StormBase> entry : bases.entrySet()) {
                
                String stormId = entry.getKey();
                StormBase base = entry.getValue();
                
                Assignment assignment = stormClusterState.assignment_info(
                        stormId, null);
                if (assignment == null) {
                    LOG.error("Failed to get assignment of " + stormId);
                    continue;
                }
                
                if (assignment.getNodeHost().containsKey(supervisorId) == false) {
                    continue;
                }
                // this assignment will use current node
                
                Map<Integer, List<TaskSummary>> portMap = new HashMap<Integer, List<TaskSummary>>();
                
                Map<Integer, NodePort> taskToNodePort = assignment
                        .getTaskToNodeport();
                for (Entry<Integer, NodePort> nodePortEntry : taskToNodePort
                        .entrySet()) {
                    Integer taskId = nodePortEntry.getKey();
                    NodePort nodePort = nodePortEntry.getValue();
                    
                    if (nodePort.getNode().equals(supervisorId) == false) {
                        continue;
                    }
                    
                    Integer port = nodePort.getPort();
                    List<TaskSummary> tasks = portMap.get(port);
                    if (tasks == null) {
                        tasks = new ArrayList<TaskSummary>();
                        portMap.put(port, tasks);
                    }
                    
                    TaskInfo taskInfo = stormClusterState.task_info(stormId,
                            taskId);
                    
                    int uptime = (int) (System.currentTimeMillis() / 1000 - assignment
                            .getTaskStartTimeSecs().get(taskId));
                    tasks.add(new TaskSummary(taskId,
                            taskInfo.getComponentId(), host, port, uptime,
                            new ArrayList<ErrorInfo>()));
                    
                }
                
                for (Entry<Integer, List<TaskSummary>> portEntry : portMap
                        .entrySet()) {
                    workers.add(new WorkerSummary(portEntry.getKey(), base
                            .getStormName(), portEntry.getValue()));
                }
                
            }
            
            supervisorSummary.set_num_used_workers(workers.size());
            
            return new SupervisorWorkers(supervisorSummary, workers);
            
        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        }
    }
    
    /**
     * Get TopologyInfo, it contain all data of the topology running status
     * 
     * @return TopologyInfo
     */
    @Override
    public TopologyInfo getTopologyInfo(String topologyId)
            throws NotAliveException, TException {
        
        TopologyInfo topologyInfo = null;
        
        StormClusterState stormClusterState = data.getStormClusterState();
        
        try {
            
            // get topology's StormBase
            StormBase base = stormClusterState.storm_base(topologyId, null);
            if (base == null) {
                throw new TException("Failed to get StormBase from ZK of "
                        + topologyId);
            }
            
            // get topology's Assignment
            Assignment assignment = stormClusterState.assignment_info(
                    topologyId, null);
            if (assignment == null) {
                throw new TException("Failed to get StormBase from ZK of "
                        + topologyId);
            }
            Map<Integer, NodePort> taskToNodePort = assignment
                    .getTaskToNodeport();
            if (taskToNodePort == null) {
                throw new TException("No taskToNodeport of " + topologyId);
            }
            
            List<TaskSummary> taskSummarys = new ArrayList<TaskSummary>();
            
            // get topology's map<taskId, componentId>
            HashMap<Integer, String> taskInfo = Cluster.topology_task_info(
                    stormClusterState, topologyId);
            for (Entry<Integer, String> entry : taskInfo.entrySet()) {
                
                Integer taskId = entry.getKey();
                String componentId = entry.getValue();
                
                NodePort np = taskToNodePort.get(taskId);
                if (np == null) {
                    LOG.warn("Topology " + topologyId + " task " + taskId
                            + " no NodePort");
                    continue;
                }
                
                // get heartbeat
                TaskHeartbeat heartbeat = stormClusterState.task_heartbeat(
                        topologyId, taskId);
                if (heartbeat == null) {
                    LOG.warn("Topology " + topologyId + " task " + taskId
                            + " no heartbeat");
                    continue;
                }
                
                String host = (String) assignment.getNodeHost().get(
                        np.getNode());
                
                List<TaskError> errors = stormClusterState.task_errors(
                        topologyId, taskId);
                List<ErrorInfo> newErrors = new ArrayList<ErrorInfo>();
                
                if (errors != null) {
                    int size = errors.size();
                    for (int i = 0; i < size; i++) {
                        TaskError e = (TaskError) errors.get(i);
                        newErrors.add(new ErrorInfo(e.getError(), e
                                .getTimSecs()));
                    }
                }
                
                int uptimeSecs = heartbeat.getUptimeSecs();
                TaskSummary taskSummary = new TaskSummary(taskId, componentId,
                        host, np.getPort(), uptimeSecs, newErrors);
                
                CommonStatsData status = (CommonStatsData) heartbeat.getStats();
                TaskStats tkStatus = status.getTaskStats();
                taskSummary.set_stats(tkStatus);
                
                taskSummarys.add(taskSummary);
            }
            
            topologyInfo = new TopologyInfo(topologyId, base.getStormName(),
                    TimeUtils.time_delta(base.getLanchTimeSecs()),
                    taskSummarys, extractStatusStr(base));
            
        } catch (TException e) {
            LOG.info("Failed to get topologyInfo" + topologyId, e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get topologyInfo" + topologyId, e);
            throw new TException("Failed to get topologyInfo" + topologyId);
        }
        
        return topologyInfo;
    }
    
    /**
     * get topology configuration
     * 
     * @param id
     *            String: topology id
     * @return String
     */
    @Override
    public String getTopologyConf(String id) throws NotAliveException,
            TException {
        String rtn;
        try {
            Map<Object, Object> topologyConf = StormConfig
                    .read_nimbus_topology_conf(conf, id);
            rtn = JStormUtils.to_json(topologyConf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.info("Failed to get configuration of " + id, e);
            throw new TException(e);
        }
        return rtn;
    }
    
    /**
     * get StormTopology throw deserialize local files
     * 
     * @param id
     *            String: topology id
     * @return StormTopology
     */
    @Override
    public StormTopology getTopology(String id) throws NotAliveException,
            TException {
        StormTopology topology = null;
        try {
            StormTopology stormtopology = StormConfig
                    .read_nimbus_topology_code(conf, id);
            if (stormtopology == null) {
                throw new TException("topology:" + id + "is null");
            }
            
            Map<Object, Object> topologyConf = (Map<Object, Object>) StormConfig
                    .read_nimbus_topology_conf(conf, id);
            
            topology = Common.system_topology(topologyConf, stormtopology);
        } catch (Exception e) {
            LOG.error("Failed to get topology " + id + ",", e);
            throw new TException("Failed to get system_topology");
        }
        return topology;
    }
    
    /**
     * Shutdown the nimbus
     */
    @Override
    public void shutdown() {
        LOG.info("Shutting down master");
        // Timer.cancelTimer(nimbus.getTimer());
        
        LOG.info("Shut down master");
        
    }
    
    @Override
    public boolean waiting() {
        // @@@ TODO
        return false;
    }
    
    /**
     * check whether the topology is bActive?
     * 
     * @param nimbus
     * @param topologyName
     * @param bActive
     * @throws Exception
     */
    public void checkTopologyActive(NimbusData nimbus, String topologyName,
            boolean bActive) throws Exception {
        if (isTopologyActive(nimbus.getStormClusterState(), topologyName) != bActive) {
            if (bActive) {
                throw new NotAliveException(topologyName + " is not alive");
            } else {
                throw new AlreadyAliveException(topologyName
                        + " is already active");
            }
        }
    }
    
    /**
     * whether the topology is active by topology name
     * 
     * @param stormClusterState
     *            see Cluster_clj
     * @param topologyName
     * @return boolean if the storm is active, return true, otherwise return
     *         false;
     * @throws Exception
     */
    public boolean isTopologyActive(StormClusterState stormClusterState,
            String topologyName) throws Exception {
        boolean rtn = false;
        if (Cluster.get_storm_id(stormClusterState, topologyName) != null) {
            rtn = true;
        }
        return rtn;
    }
    
    /**
     * create local topology files /local-dir/nimbus/topologyId/stormjar.jar
     * /local-dir/nimbus/topologyId/stormcode.ser
     * /local-dir/nimbus/topologyId/stormconf.ser
     * 
     * @param conf
     * @param topologyId
     * @param tmpJarLocation
     * @param stormConf
     * @param topology
     * @throws IOException
     */
    private void setupStormCode(Map<Object, Object> conf, String topologyId,
            String tmpJarLocation, Map<Object, Object> stormConf,
            StormTopology topology) throws IOException {
        // local-dir/nimbus/stormdist/topologyId
        String stormroot = StormConfig.masterStormdistRoot(conf, topologyId);
        
        FileUtils.forceMkdir(new File(stormroot));
        FileUtils.cleanDirectory(new File(stormroot));
        
        // copy jar to /local-dir/nimbus/topologyId/stormjar.jar
        setupJar(conf, tmpJarLocation, stormroot);
        
        // serialize to file /local-dir/nimbus/topologyId/stormcode.ser
        FileUtils.writeByteArrayToFile(
                new File(StormConfig.stormcode_path(stormroot)),
                Utils.serialize(topology));
        
        // serialize to file /local-dir/nimbus/topologyId/stormconf.ser
        FileUtils.writeByteArrayToFile(
                new File(StormConfig.sotrmconf_path(stormroot)),
                Utils.serialize(stormConf));
    }
    
    /**
     * Copy jar to /local-dir/nimbus/topologyId/stormjar.jar
     * 
     * @param conf
     * @param tmpJarLocation
     * @param stormroot
     * @throws IOException
     */
    private void setupJar(Map<Object, Object> conf, String tmpJarLocation,
            String stormroot) throws IOException {
        File srcFile = new File(tmpJarLocation);
        if (!srcFile.exists()) {
            throw new IllegalArgumentException(tmpJarLocation + " to copy to "
                    + stormroot + " does not exist!");
        }
        String path = StormConfig.stormjar_path(stormroot);
        File destFile = new File(path);
        FileUtils.copyFile(srcFile, destFile);
    }
    
    /**
     * generate TaskInfo for every bolt or spout in ZK /ZK/tasks/topoologyId/xxx
     * 
     * @param conf
     * @param stormId
     * @param stormClusterState
     * @throws Exception
     */
    public void setupZkTaskInfo(Map<Object, Object> conf, String topologyId,
            StormClusterState stormClusterState) throws Exception {
        
        // mkdir /ZK/taskbeats/topoologyId
        stormClusterState.setup_heartbeats(topologyId);
        
        Map<Integer, String> taskToComponetId = mkTaskComponentAssignments(
                conf, topologyId);
        if (taskToComponetId == null) {
            throw new InvalidTopologyException("Failed to generate TaskIDs map");
        }
        
        for (Entry<Integer, String> entry : taskToComponetId.entrySet()) {
            // key is taskid, value is taskinfo
            
            TaskInfo taskinfo = new TaskInfo(entry.getValue());
            
            stormClusterState.set_task(topologyId, entry.getKey(), taskinfo);
        }
    }
    
    /**
     * generate a taskid(Integer) for every task
     * 
     * @param conf
     * @param topologyid
     * @return Map<Integer, String>: from taskid to componentid
     * @throws IOException
     * @throws InvalidTopologyException
     */
    public Map<Integer, String> mkTaskComponentAssignments(
            Map<Object, Object> conf, String topologyid) throws IOException,
            InvalidTopologyException {
        
        // @@@ here exist a little problem,
        // we can directly pass stormConf from Submit method
        Map<Object, Object> stormConf = StormConfig.read_nimbus_topology_conf(
                conf, topologyid);
        
        StormTopology stopology = StormConfig.read_nimbus_topology_code(conf,
                topologyid);
        
        Map<Integer, String> rtn = new HashMap<Integer, String>();
        
        StormTopology topology = Common.system_topology(stormConf, stopology);
        
        Integer count = 0;
        count = mkTaskMaker(stormConf, topology.get_bolts(), rtn, count);
        count = mkTaskMaker(stormConf, topology.get_spouts(), rtn, count);
        count = mkTaskMaker(stormConf, topology.get_state_spouts(), rtn, count);
        
        return rtn;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Integer mkTaskMaker(Map<Object, Object> stormConf,
            Map<String, ?> cidSpec, Map<Integer, String> rtn, Integer cnt) {
        if (cidSpec == null) {
            LOG.warn("Component map is empty");
            return cnt;
        }
        
        Set<?> entrySet = cidSpec.entrySet();
        for (Iterator<?> it = entrySet.iterator(); it.hasNext();) {
            Entry entry = (Entry) it.next();
            Object obj = entry.getValue();
            
            ComponentCommon common = null;
            if (obj instanceof Bolt) {
                common = ((Bolt) obj).get_common();
                
            } else if (obj instanceof SpoutSpec) {
                common = ((SpoutSpec) obj).get_common();
                
            } else if (obj instanceof SpoutSpec) {
                common = ((StateSpoutSpec) obj).get_common();
                
            }
            
            int declared = Thrift.parallelismHint(common);
            Integer parallelism = declared;
            // Map tmp = (Map) Utils_clj.from_json(common.get_json_conf());
            
            Map newStormConf = new HashMap(stormConf);
            // newStormConf.putAll(tmp);
            Integer maxParallelism = (Integer) newStormConf
                    .get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
            if (maxParallelism != null) {
                parallelism = Math.min(maxParallelism, declared);
            }
            
            for (int i = 0; i < parallelism; i++) {
                cnt++;
                rtn.put(cnt, (String) entry.getKey());
            }
        }
        return cnt;
    }
    
    public String extractStatusStr(StormBase stormBase) {
        StatusType t = stormBase.getStatus().getStatusType();
        return t.getStatus().toUpperCase();
    }
    
}
