package com.alipay.dw.jstorm.daemon.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import com.alipay.dw.jstorm.client.ConfigExtension;
import com.alipay.dw.jstorm.cluster.Cluster;
import com.alipay.dw.jstorm.cluster.ClusterState;
import com.alipay.dw.jstorm.cluster.Common;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.daemon.nimbus.StatusType;
import com.alipay.dw.jstorm.task.TaskShutdownDameon;
import com.alipay.dw.jstorm.utils.PathUtils;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

public class WorkerData {
    private static Logger                                LOG      = Logger.getLogger(WorkerData.class);
    
    // system configuration
    
    private Map<Object, Object>                          conf;
    // worker configuration
    
    private Map<Object, Object>                          stormConf;
    
    // message queue
    private MQContext                                    mqContext;
    
    private final String                                 topologyId;
    private final String                                 supervisorId;
    private final Integer                                port;
    private final String                                 workerId;
    
    // worker status :active/deactive
    private AtomicBoolean                                active;
    // Topology status
    private StatusType                                   topologyStatus;
    
    // ZK interface
    private ClusterState                                 zkClusterstate;
    private StormClusterState                            zkCluster;
    
    // running taskId list in current worker
    private Set<Integer>                                 taskids;
    // connection to other workers  <NodePort, ZMQConnection>
    private ConcurrentHashMap<NodePort, ISendConnection> nodeportSocket;
    // <taskId, NodePort>
    private ConcurrentHashMap<Integer, NodePort>         taskNodeport;
    // <taskId, component>
    private HashMap<Integer, String>                     tasksToComponent;
    
    // raw topology is deserialized from local jar
    // it doesn't contain acker
    private StormTopology                                rawTopology;
    // sys topology is the running topology in the worker
    // it contain ackers
    private StormTopology                                sysTopology;
    
    private ContextMaker                                 contextMaker;
    
    // shutdown woker entrance
    private final WorkerHaltRunable                      workHalt = new WorkerHaltRunable();
    
    // sending tuple's queue
    private LinkedBlockingQueue<TransferData>         transferQueue;
    
    private List<TaskShutdownDameon>                     shutdownTasks;
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public WorkerData(Map conf, MQContext mq_context, String topology_id,
            String supervisor_id, int port, String worker_id) throws Exception {
        
        this.conf = conf;
        this.mqContext = mq_context;
        this.topologyId = topology_id;
        this.supervisorId = supervisor_id;
        this.port = port;
        this.workerId = worker_id;
        
        this.active = new AtomicBoolean(true);
        this.topologyStatus = StatusType.active;
        
        if (StormConfig.cluster_mode(conf).equals("distributed")) {
            String pid = JStormUtils.process_pid();
            String pidPath = StormConfig.worker_pid_path(conf, worker_id, pid);
            PathUtils.touch(pidPath);
            LOG.info("Current worker's pid is " + pidPath);
        }
        
        // create zk interface
        this.zkClusterstate = Cluster.mk_distributed_cluster_state(conf);
        this.zkCluster = Cluster.mk_storm_cluster_state(zkClusterstate);
        
        
        Map rawConf = StormConfig.read_supervisor_topology_conf(conf,
                topology_id);
        this.stormConf = new HashMap<Object, Object>();
        this.stormConf.putAll(conf);
        this.stormConf.putAll(rawConf);
        
        // create zeroMQ instance
        if (this.mqContext == null) {
            int zmqThreads = JStormUtils.parseInt(stormConf
                    .get(Config.ZMQ_THREADS));
            
            int linger = JStormUtils.parseInt(stormConf
                    .get(Config.ZMQ_LINGER_MILLIS));
            
            int maxQueueMsg = JStormUtils.parseInt(stormConf.
                    get(ConfigExtension.ZMQ_MAX_QUEUE_MSG), 
                    ConfigExtension.DEFAULT_ZMQ_MAX_QUEUE_MSG);
            
            boolean isLocal = StormConfig.cluster_mode(conf).equals("local");
            
            // virtport ZMQ will define whether use ZMQ in worker internal commnication
            boolean virtportZmq = JStormUtils.parseBoolean(
                    stormConf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
            this.mqContext = MQContext.mk_zmq_context(zmqThreads, linger,
                    isLocal, virtportZmq, maxQueueMsg);
        }
        
        this.nodeportSocket = new ConcurrentHashMap<NodePort, ISendConnection>();
        this.taskNodeport = new ConcurrentHashMap<Integer, NodePort>();
        
        // get current worker's task list
        this.taskids = Cluster.readWorkerTaskids(zkCluster, topologyId,
                supervisorId, port);
        if (taskids == null || taskids.size() == 0) {
            throw new RuntimeException("No tasks running current workers");
        }
        LOG.info("Current worker taskList:" + taskids);
        
        this.tasksToComponent = Cluster.topology_task_info(zkCluster,
                topologyId);
        LOG.info("Map<taskId, component>:" + tasksToComponent);
        
        // deserialize topology code from local dir
        rawTopology = StormConfig.read_supervisor_topology_code(conf,
                topology_id);
        sysTopology = Common.system_topology(stormConf, rawTopology);
        
        contextMaker = new ContextMaker(stormConf, topology_id, worker_id,
                tasksToComponent, port, JStormUtils.mk_list(taskids));
        
        transferQueue = new LinkedBlockingQueue<TransferData>();
        
        LOG.info("Successfully create WorkerData");
        
    }
    
    public Map<Object, Object> getConf() {
        return conf;
    }
    
    public AtomicBoolean getActive() {
        return active;
    }
    
    public void setActive(AtomicBoolean active) {
        this.active = active;
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
    
    public MQContext getMqContext() {
        return mqContext;
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
    
    public ConcurrentHashMap<NodePort, ISendConnection> getNodeportSocket() {
        return nodeportSocket;
    }
    
    public ConcurrentHashMap<Integer, NodePort> getTaskNodeport() {
        return taskNodeport;
    }
    
    public HashMap<Integer, String> getTasksToComponent() {
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
    
    public WorkerHaltRunable getWorkHalt() {
        return workHalt;
    }
    
    public LinkedBlockingQueue<TransferData> getTransferQueue() {
        return transferQueue;
    }
    
    
    public List<TaskShutdownDameon> getShutdownTasks() {
        return shutdownTasks;
    }
    
    public void setShutdownTasks(List<TaskShutdownDameon> shutdownTasks) {
        this.shutdownTasks = shutdownTasks;
    }
}
