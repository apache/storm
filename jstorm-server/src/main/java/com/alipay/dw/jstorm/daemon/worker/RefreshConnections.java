package com.alipay.dw.jstorm.daemon.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.task.Assignment;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

/**
 * 
 * Update current worker and other workers' zeroMQ connection.
 * 
 * When worker shutdown/create, need update these connection
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshConnections extends RunnableCallback {
    private static Logger                                LOG = Logger.getLogger(RefreshConnections.class);
    
    private WorkerData                                   workerData;
    
    private AtomicBoolean                                active;
    
    @SuppressWarnings("rawtypes")
    private Map                                          conf;
    
    private StormClusterState                            zkCluster;
    
    private String                                       topologyId;
    
    private Set<Integer>                                 outboundTasks;
    
    private ConcurrentHashMap<NodePort, ISendConnection> nodeportSocket;
    
    private MQContext                                    mqContext;
    
    private ConcurrentHashMap<Integer, NodePort>         taskNodeport;
    
    private Integer                                      frequence;
    
    // private ReentrantReadWriteLock endpoint_socket_lock;
    
    @SuppressWarnings("rawtypes")
    public RefreshConnections(WorkerData workerData, Set<Integer> outbound_tasks) {
        
        this.workerData = workerData;
        
        this.active = workerData.getActive();
        this.conf = workerData.getConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.outboundTasks = outbound_tasks;
        this.nodeportSocket = workerData.getNodeportSocket();
        this.mqContext = workerData.getMqContext();
        this.taskNodeport = workerData.getTaskNodeport();
        
        // this.endpoint_socket_lock = endpoint_socket_lock;
        frequence = JStormUtils.parseInt(
                conf.get(Config.TASK_REFRESH_POLL_SECS), 5);
    }
    
    @Override
    public void run() {
        
        if (active.get() == false) {
            return ;
        }
        
        try {
            //
            // @@@ does lock need?
            // endpoint_socket_lock.writeLock().lock();
            //
            
            // get assignment and register himself to ZK
            // @@@ need register watch??
            Assignment assignment = zkCluster.assignment_info(topologyId, this);
            if (assignment == null) {
                String errMsg = "Failed to get Assignment of " + topologyId;
                LOG.error(errMsg);
                //throw new RuntimeException(errMsg);
                return ;
            }
            
            Map<Integer, NodePort> my_assignment = new HashMap<Integer, NodePort>();
            
            Map<Integer, NodePort> taskNodeportAll = assignment
                    .getTaskToNodeport();
            
            Map<String, String> node = assignment.getNodeHost();
            
            // only reserve outboundTasks
            Set<NodePort> need_connections = new HashSet<NodePort>();
            
            if (taskNodeportAll != null && outboundTasks != null) {
                for (Entry<Integer, NodePort> mm : taskNodeportAll.entrySet()) {
                    int taks_id = mm.getKey();
                    if (outboundTasks.contains(taks_id)) {
                        my_assignment.put(taks_id, mm.getValue());
                        need_connections.add(mm.getValue());
                    }
                }
            }
            taskNodeport.putAll(my_assignment);
            
            // get which connection need to be remove or add
            Set<NodePort> current_connections = nodeportSocket.keySet();
            Set<NodePort> new_connections = new HashSet<NodePort>();
            Set<NodePort> remove_connections = new HashSet<NodePort>();
            
            for (NodePort node_port : need_connections) {
                if (!current_connections.contains(node_port)) {
                    new_connections.add(node_port);
                }
            }
            
            for (NodePort node_port : current_connections) {
                if (!need_connections.contains(node_port)) {
                    remove_connections.add(node_port);
                }
            }
            
            // create new connection
            for (NodePort nodePort : new_connections) {
                
                String host = node.get(nodePort.getNode());
                
                int port = nodePort.getPort();
                
                // @@@ if failed to connect, does it throw exception?
                ISendConnection conn = mqContext.connect(true, host, port);
                
                nodeportSocket.put(nodePort, conn);
                
                LOG.info("Add connection to " + nodePort);
            }
            
            // close useless connection
            for (NodePort node_port : remove_connections) {
                LOG.info("Remove connection to " + node_port);
                nodeportSocket.remove(node_port).close();
            }
        } catch (Exception e) {
            LOG.error("Failed to refresh worker Connection", e);
        }
        
        // finally {
        // endpoint_socket_lock.writeLock().unlock();
        // }
        
    }
    
    @Override
    public Object getResult() {
        if (active.get()) {
            return frequence;
        }
        return -1;
    }
    
}
