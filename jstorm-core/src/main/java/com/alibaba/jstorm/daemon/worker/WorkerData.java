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
package com.alibaba.jstorm.daemon.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.message.netty.ControlMessage;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.zk.ZkTool;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;


public class WorkerData {

    private static Logger LOG = LoggerFactory.getLogger(WorkerData.class);

    // system configuration

    private Map<Object, Object> conf;
    // worker configuration

    private Map<Object, Object> stormConf;

    // message queue
    private IContext context;

    private final String topologyId;
    private final String supervisorId;
    private final Integer port;
    private final String workerId;
    // worker status :active/shutdown
    private AtomicBoolean shutdown;
    private AtomicBoolean monitorEnable;

    // Topology status
    private StatusType topologyStatus;

    // ZK interface
    private ClusterState zkClusterstate;
    private StormClusterState zkCluster;

    // running taskId list in current worker
    private Set<Integer> taskids;

    // connection to other workers <NodePort, ZMQConnection>
    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    // <taskId, NodePort>
    private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    private ConcurrentSkipListSet<ResourceWorkerSlot> workerToResource;

    private volatile Set<Integer> outboundTasks;
    private Set<Integer> localTasks;

    private ConcurrentHashMap<Integer, DisruptorQueue> innerTaskTransfer;
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    // <taskId, component>
    private ConcurrentHashMap<Integer, String> tasksToComponent;

    private ConcurrentHashMap<String, List<Integer>> componentToSortedTasks;

    private Map<String, Object> defaultResources;
    private Map<String, Object> userResources;
    private Map<String, Object> executorData;
    private Map registeredMetrics;

    // raw topology is deserialized from local jar
    // it doesn't contain acker
    private StormTopology rawTopology;
    // sys topology is the running topology in the worker
    // it contain ackers
    private StormTopology sysTopology;

    private ContextMaker contextMaker;

    // shutdown woker entrance
    private final AsyncLoopDefaultKill workHalt = new AsyncLoopDefaultKill();

    // sending tuple's queue
    // private LinkedBlockingQueue<TransferData> transferQueue;
    private DisruptorQueue transferQueue;

    private DisruptorQueue sendingQueue;

    private List<TaskShutdownDameon> shutdownTasks;

    private ConcurrentHashMap<Integer, Boolean> outTaskStatus; // true => active

    public static final int THREAD_POOL_NUM = 4;
    private ScheduledExecutorService threadPool;

    private volatile Long assignmentTS; // Assignment timeStamp. The time of
                                        // last update of assignment
    private volatile AssignmentType assignmentType;
    
    private IConnection recvConnection;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public WorkerData(Map conf, IContext context, String topology_id,
            String supervisor_id, int port, String worker_id, String jar_path)
            throws Exception {

        this.conf = conf;
        this.context = context;
        this.topologyId = topology_id;
        this.supervisorId = supervisor_id;
        this.port = port;
        this.workerId = worker_id;

        this.shutdown = new AtomicBoolean(false);
        
        this.monitorEnable = new AtomicBoolean(true);
        this.topologyStatus = StatusType.active;

        if (StormConfig.cluster_mode(conf).equals("distributed")) {
            String pidDir = StormConfig.worker_pids_root(conf, worker_id);
            JStormServerUtils.createPid(pidDir);
        }

        // create zk interface
        this.zkClusterstate = ZkTool.mk_distributed_cluster_state(conf);
        this.zkCluster = Cluster.mk_storm_cluster_state(zkClusterstate);

        Map rawConf =
                StormConfig.read_supervisor_topology_conf(conf, topology_id);
        this.stormConf = new HashMap<Object, Object>();
        this.stormConf.putAll(conf);
        this.stormConf.putAll(rawConf);
        
        ConfigExtension.setLocalSupervisorId(stormConf, supervisorId);
        ConfigExtension.setLocalWorkerId(stormConf, workerId);
        ConfigExtension.setLocalWorkerPort(stormConf, port);
        ControlMessage.setPort(port);
        Metric.setEnable(ConfigExtension.isEnablePerformanceMetrics(stormConf));

        LOG.info("Worker Configuration " + stormConf);

        try {

            boolean enableClassloader =
                    ConfigExtension.isEnableTopologyClassLoader(stormConf);
            boolean enableDebugClassloader =
                    ConfigExtension.isEnableClassloaderDebug(stormConf);

            if (jar_path == null && enableClassloader == true
                    && !conf.get(Config.STORM_CLUSTER_MODE).equals("local")) {
                LOG.error("enable classloader, but not app jar");
                throw new InvalidParameterException();
            }

            URL[] urlArray = new URL[0];
            if (jar_path != null) {
                String[] paths = jar_path.split(":");
                Set<URL> urls = new HashSet<URL>();
                for (String path : paths) {
                    if (StringUtils.isBlank(path))
                        continue;
                    URL url = new URL("File:" + path);
                    urls.add(url);
                }
                urlArray = urls.toArray(new URL[0]);

            }

            WorkerClassLoader.mkInstance(urlArray, ClassLoader
                    .getSystemClassLoader(), ClassLoader.getSystemClassLoader()
                    .getParent(), enableClassloader, enableDebugClassloader);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("init jarClassLoader error!", e);
            throw new InvalidParameterException();
        }

        if (this.context == null) {
            this.context = TransportFactory.makeContext(stormConf);
        }

        boolean disruptorUseSleep =
                ConfigExtension.isDisruptorUseSleep(stormConf);
        DisruptorQueue.setUseSleep(disruptorUseSleep);
        boolean isLimited =
                ConfigExtension.getTopologyBufferSizeLimited(stormConf);
        DisruptorQueue.setLimited(isLimited);
        LOG.info("Disruptor use sleep:" + disruptorUseSleep + ", limited size:"
                + isLimited);

        // this.transferQueue = new LinkedBlockingQueue<TransferData>();
        int buffer_size =
                Utils.getInt(conf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        WaitStrategy waitStrategy =
                (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(conf);
        this.transferQueue =
                DisruptorQueue.mkInstance("TotalTransfer", ProducerType.MULTI,
                        buffer_size, waitStrategy);
        this.transferQueue.consumerStarted();
        this.sendingQueue =
                DisruptorQueue.mkInstance("TotalSending", ProducerType.MULTI,
                        buffer_size, waitStrategy);
        this.sendingQueue.consumerStarted();

        this.nodeportSocket = new ConcurrentHashMap<WorkerSlot, IConnection>();
        this.taskNodeport = new ConcurrentHashMap<Integer, WorkerSlot>();
        this.workerToResource = new ConcurrentSkipListSet<ResourceWorkerSlot>();
        this.innerTaskTransfer =
                new ConcurrentHashMap<Integer, DisruptorQueue>();
        this.deserializeQueues =
                new ConcurrentHashMap<Integer, DisruptorQueue>();
        this.tasksToComponent = new ConcurrentHashMap<Integer, String>();
        this.componentToSortedTasks =
                new ConcurrentHashMap<String, List<Integer>>();

        Assignment assignment = zkCluster.assignment_info(topologyId, null);
        if (assignment == null) {
            String errMsg = "Failed to get Assignment of " + topologyId;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        workerToResource.addAll(assignment.getWorkers());

        // get current worker's task list

        this.taskids = assignment.getCurrentWorkerTasks(supervisorId, port);
        if (taskids.size() == 0) {
            throw new RuntimeException("No tasks running current workers");
        }
        LOG.info("Current worker taskList:" + taskids);

        // deserialize topology code from local dir
        rawTopology =
                StormConfig.read_supervisor_topology_code(conf, topology_id);
        sysTopology = Common.system_topology(stormConf, rawTopology);

        generateMaps();

        contextMaker = new ContextMaker(this);

        outTaskStatus = new ConcurrentHashMap<Integer, Boolean>();

        threadPool = Executors.newScheduledThreadPool(THREAD_POOL_NUM);
        TimerTrigger.setScheduledExecutorService(threadPool);

        try {
            Long tmp =
                    StormConfig.read_supervisor_topology_timestamp(conf,
                            topology_id);
            assignmentTS = (tmp == null ? System.currentTimeMillis() : tmp);
        } catch (FileNotFoundException e) {
            assignmentTS = System.currentTimeMillis();
        }
        
        outboundTasks = new HashSet<Integer>();

        LOG.info("Successfully create WorkerData");

    }

    /**
     * private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport; private
     * HashMap<Integer, String> tasksToComponent; private Map<String,
     * List<Integer>> componentToSortedTasks; private Map<String, Map<String,
     * Fields>> componentToStreamToFields; private Map<String, Object>
     * defaultResources; private Map<String, Object> userResources; private
     * Map<String, Object> executorData; private Map registeredMetrics;
     * 
     * @throws Exception
     */
    private void generateMaps() throws Exception {
        updateTaskComponentMap();

        this.defaultResources = new HashMap<String, Object>();
        this.userResources = new HashMap<String, Object>();
        this.executorData = new HashMap<String, Object>();
        this.registeredMetrics = new HashMap();
    }

    public Map<Object, Object> getConf() {
        return conf;
    }

    public AtomicBoolean getShutdown() {
        return shutdown;
    }

    public StatusType getTopologyStatus() {
        return topologyStatus;
    }

    public void setTopologyStatus(StatusType topologyStatus) {
        this.topologyStatus = topologyStatus;
    }

    public Map<Object, Object> getStormConf() {
        return stormConf;
    }

    public IContext getContext() {
        return context;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public Integer getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    public ClusterState getZkClusterstate() {
        return zkClusterstate;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Set<Integer> getTaskids() {
        return taskids;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodeportSocket() {
        return nodeportSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskNodeport() {
        return taskNodeport;
    }

    public ConcurrentSkipListSet<ResourceWorkerSlot> getWorkerToResource() {
        return workerToResource;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getInnerTaskTransfer() {
        return innerTaskTransfer;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getDeserializeQueues() {
        return deserializeQueues;
    }

    public ConcurrentHashMap<Integer, String> getTasksToComponent() {
        return tasksToComponent;
    }

    public StormTopology getRawTopology() {
        return rawTopology;
    }

    public StormTopology getSysTopology() {
        return sysTopology;
    }

    public ContextMaker getContextMaker() {
        return contextMaker;
    }

    public AsyncLoopDefaultKill getWorkHalt() {
        return workHalt;
    }

    public DisruptorQueue getTransferQueue() {
        return transferQueue;
    }

    // public LinkedBlockingQueue<TransferData> getTransferQueue() {
    // return transferQueue;
    // }

    public DisruptorQueue getSendingQueue() {
        return sendingQueue;
    }

    public Map<String, List<Integer>> getComponentToSortedTasks() {
        return componentToSortedTasks;
    }

    public Map<String, Object> getDefaultResources() {
        return defaultResources;
    }

    public Map<String, Object> getUserResources() {
        return userResources;
    }

    public Map<String, Object> getExecutorData() {
        return executorData;
    }

    public Map getRegisteredMetrics() {
        return registeredMetrics;
    }

    public List<TaskShutdownDameon> getShutdownTasks() {
        return shutdownTasks;
    }

    public void setShutdownTasks(List<TaskShutdownDameon> shutdownTasks) {
        this.shutdownTasks = shutdownTasks;
    }

    public void addShutdownTask(TaskShutdownDameon shutdownTask) {
        this.shutdownTasks.add(shutdownTask);
    }

    public List<TaskShutdownDameon> getShutdownDaemonbyTaskIds(
            Set<Integer> taskIds) {
        List<TaskShutdownDameon> ret = new ArrayList<TaskShutdownDameon>();
        for (TaskShutdownDameon shutdown : shutdownTasks) {
            if (taskIds.contains(shutdown.getTaskId()))
                ret.add(shutdown);
        }
        return ret;
    }

    public Set<Integer> getLocalTasks() {
        return localTasks;
    }

    public void setLocalTasks(Set<Integer> localTasks) {
        this.localTasks = localTasks;
    }

    public void initOutboundTaskStatus(Set<Integer> outboundTasks) {
        for (Integer taskId : outboundTasks) {
            outTaskStatus.put(taskId, false);
        }
    }
    
    public Map<Integer, Boolean> getOutboundTaskStatus() {
        return outTaskStatus;
    }

    public void addOutboundTaskStatusIfAbsent(Integer taskId) {
        outTaskStatus.putIfAbsent(taskId, false);
    }
    
    public void removeOutboundTaskStatus(Integer taskId) {
        outTaskStatus.remove(taskId);
    }

    public void updateOutboundTaskStatus(Integer taskId, boolean isActive) {
        outTaskStatus.put(taskId, isActive);
    }

    public boolean isOutboundTaskActive(Integer taskId) {
        return outTaskStatus.get(taskId) != null ? outTaskStatus.get(taskId)
                : false;
    }

    public ScheduledExecutorService getThreadPool() {
        return threadPool;
    }

    public void setAssignmentTs(Long time) {
        assignmentTS = time;
    }

    public Long getAssignmentTs() {
        return assignmentTS;
    }
    
    public void setAssignmentType(AssignmentType type) {
        this.assignmentType = type;
    }
    
    public AssignmentType getAssignmentType() {
        return assignmentType;
    }

    public void updateWorkerData(Assignment assignment) throws Exception {
        updateTaskIds(assignment);
        updateTaskComponentMap();
        updateStormTopology();
    }

    public void updateTaskIds(Assignment assignment) {
        this.taskids.clear();
        this.taskids.addAll(assignment
                .getCurrentWorkerTasks(supervisorId, port));
    }

    public void setOutboundTasks(Set<Integer> outboundTasks) {
        this.outboundTasks = outboundTasks;
    }
    
    public Set<Integer> getOutboundTasks() {
        return outboundTasks;
    }

    private void updateTaskComponentMap() throws Exception {
        Map<Integer, String> tmp = Common.getTaskToComponent(
                        Cluster.get_all_taskInfo(zkCluster, topologyId));

        this.tasksToComponent.putAll(tmp);
        LOG.info("Updated tasksToComponentMap:" + tasksToComponent);

        this.componentToSortedTasks.putAll(JStormUtils.reverse_map(tmp));
        for (java.util.Map.Entry<String, List<Integer>> entry : componentToSortedTasks
                .entrySet()) {
            List<Integer> tasks = entry.getValue();

            Collections.sort(tasks);
        }
    }

    private void updateStormTopology() {
        StormTopology rawTmp = null;
        StormTopology sysTmp = null;

        try {
            rawTmp =
                    StormConfig.read_supervisor_topology_code(conf, topologyId);
            sysTmp = Common.system_topology(stormConf, rawTopology);
        } catch (IOException e) {
            LOG.error("Failed to read supervisor topology code for "
                    + topologyId, e);
            return;
        } catch (InvalidTopologyException e) {
            LOG.error("Failed to update sysTopology for " + topologyId, e);
            return;
        }

        updateTopology(rawTopology, rawTmp);
        updateTopology(sysTopology, sysTmp);
    }

    private void updateTopology(StormTopology oldTopology,
            StormTopology newTopology) {
        oldTopology.set_bolts(newTopology.get_bolts());
        oldTopology.set_spouts(newTopology.get_spouts());
        oldTopology.set_state_spouts(newTopology.get_state_spouts());
    }

    public AtomicBoolean getMonitorEnable() {
        return monitorEnable;
    }

	public IConnection getRecvConnection() {
		return recvConnection;
	}

	public void setRecvConnection(IConnection recvConnection) {
		this.recvConnection = recvConnection;
	}

}
