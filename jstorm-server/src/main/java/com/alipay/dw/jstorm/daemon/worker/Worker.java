package com.alipay.dw.jstorm.daemon.worker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.task.Task;
import com.alipay.dw.jstorm.task.TaskShutdownDameon;
import com.alipay.dw.jstorm.zeroMq.MQContext;

/**
 * worker entrance
 * 
 * @author yannian/Longda
 * 
 */
public class Worker {
    
    private static Logger LOG = Logger.getLogger(Worker.class);
    
    /**
     * Why need workerData, it is for thread comeptition
     */
    private WorkerData    workerData;
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Worker(Map conf, MQContext mq_context, String topology_id,
            String supervisor_id, int port, String worker_id) throws Exception {
        
        workerData = new WorkerData(conf, mq_context, topology_id,
                supervisor_id, port, worker_id);
        
    }
    
    /**
     * get current task's output task list
     * 
     * @param tasks_component
     * @param mk_topology_context
     * @param task_ids
     * @throws Exception
     */
    public Set<Integer> worker_output_tasks() {
        
        ContextMaker context_maker = workerData.getContextMaker();
        Set<Integer> task_ids = workerData.getTaskids();
        StormTopology topology = workerData.getSysTopology();
        
        Set<Integer> rtn = new HashSet<Integer>();
        
        for (Integer taskid : task_ids) {
            TopologyContext context = context_maker.makeTopologyContext(
                    topology, taskid);
            
            // <StreamId, <ComponentId, Grouping>>
            Map<String, Map<String, Grouping>> targets = context
                    .getThisTargets();
            for (Map<String, Grouping> e : targets.values()) {
                for (String componentId : e.keySet()) {
                    List<Integer> tasks = context
                            .getComponentTasks(componentId);
                    rtn.addAll(tasks);
                }
            }
        }
        
        return rtn;
    }
    
    private RefreshConnections makeRefreshConnections() {
        
        // get output streams of every task
        Set<Integer> outboundTasks = worker_output_tasks();
        
        RefreshConnections refresh_connections = new RefreshConnections(
                workerData, outboundTasks);
        
        return refresh_connections;
    }
    
    private WorkerTransfer getSendingTransfer() {
        TopologyContext sysTopologyContext = workerData.getContextMaker()
                .makeTopologyContext(workerData.getSysTopology(), null);
        
        // sending tuple's serializer
        KryoTupleSerializer serializer = new KryoTupleSerializer(
                workerData.getStormConf(), sysTopologyContext);
        
        // Task sending all tuples through this Object
        return new WorkerTransfer(serializer, workerData.getTransferQueue());
    }
    
    private List<TaskShutdownDameon> createTasks(WorkerTransfer workerTransfer)
            throws Exception {
        List<TaskShutdownDameon> shutdowntasks = new ArrayList<TaskShutdownDameon>();
        
        Set<Integer> taskids = workerData.getTaskids();
        
        for (int taskid : taskids) {
            
            TaskShutdownDameon t = Task.mk_task(workerData, workerTransfer,
                    taskid);
            
            shutdowntasks.add(t);
        }
        
        return shutdowntasks;
    }
    
    public WorkerShutdown execute() throws Exception {
        
        WorkerTransfer workerTransfer = getSendingTransfer();
        
        // shutdown task callbacks
        List<TaskShutdownDameon> shutdowntasks = createTasks(workerTransfer);
        workerData.setShutdownTasks(shutdowntasks);
        
        // create virtual port object
        // when worker receives tupls, dispatch targetTask according to task_id
        // conf, supervisorId, topologyId, port, mqContext, taskids
        WorkerVirtualPort virtual_port = new WorkerVirtualPort(workerData);
        Shutdownable virtual_port_shutdown = virtual_port.launch();
        
        // refresh connection
        RefreshConnections refreshConn = makeRefreshConnections();
        AsyncLoopThread refreshconn = new AsyncLoopThread(refreshConn);
        
        // refresh ZK active status
        RefreshActive refreshZkActive = new RefreshActive(workerData);
        AsyncLoopThread refreshzk = new AsyncLoopThread(refreshZkActive);
        
        // refresh hearbeat to Local dir
        RunnableCallback heartbeat_fn = new WorkerHeartbeatRunable(workerData);
        AsyncLoopThread hb = new AsyncLoopThread(heartbeat_fn, false, null,
                Thread.NORM_PRIORITY, true);
        
        // transferQueue, nodeportSocket, taskNodeport
        DrainerRunable drainer = new DrainerRunable(workerData);
        AsyncLoopThread dr = new AsyncLoopThread(drainer, false, null,
                Thread.MAX_PRIORITY, true);
        
        AsyncLoopThread[] threads = { refreshconn, refreshzk, hb, dr };
        
        return new WorkerShutdown(workerData, shutdowntasks,
                virtual_port_shutdown, threads);
        
    }
    
    /**
     * create worker instance and run it
     * 
     * @param conf
     * @param mq_context
     * @param topology_id
     * @param supervisor_id
     * @param port
     * @param worker_id
     * @return
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public static WorkerShutdown mk_worker(Map conf, MQContext mq_context,
            String topology_id, String supervisor_id, int port, String worker_id)
            throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("topologyId:" + topology_id + ", ");
        sb.append("port:" + port + ", ");
        sb.append("workerId:" + worker_id + "\n");
        sb.append("Configuration:" + conf);
        
        LOG.info("Begin to run worker:" + sb.toString());
        
        Worker w = new Worker(conf, mq_context, topology_id, supervisor_id,
                port, worker_id);
        
        return w.execute();
    }
    
    /**
     * worker entrance
     * 
     * @param args
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        if (args.length != 4) {
            LOG.error("the length of args is less than 4");
            return;
        }
        String topology_id = args[0];
        String supervisor_id = args[1];
        String port_str = args[2];
        String worker_id = args[3];
        
        Map conf = Utils.readStormConfig();
        StormConfig.validate_distributed_mode(conf);
        
        StringBuilder sb = new StringBuilder();
        sb.append("topologyId:" + topology_id + ", ");
        sb.append("port:" + port_str + ", ");
        sb.append("workerId:" + worker_id + "\n");
        
        try {
            WorkerShutdown sd = mk_worker(conf, null, topology_id,
                    supervisor_id, Integer.parseInt(port_str), worker_id);
            sd.join();
            
            LOG.info("Successfully shutdown worker " + sb.toString());
        } catch (Exception e) {
            String errMsg = "Failed to create worker, " + sb.toString();
            LOG.error(errMsg, e);
        }
    }
    
}
