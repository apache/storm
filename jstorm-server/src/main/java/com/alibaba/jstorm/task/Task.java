package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import clojure.lang.Atom;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerHaltRunable;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.execute.SpoutExecutors;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Task instance
 * 
 * @author yannian/Longda
 * 
 */
public class Task {
    
    private final static Logger LOG    = Logger.getLogger(Task.class);
    
    private Map<Object, Object> stormConf;
    
    private TopologyContext     topologyContext;
    private TopologyContext     userContext;
    private String              topologyid;
    private IContext            context;
    
    private TaskTransfer        taskTransfer;
    private Map<Integer, DisruptorQueue> innerTaskTransfer;
    private WorkerHaltRunable   workHalt;
    
    private Integer             taskid;
    private String              componentid;
    private TaskStatus          taskStatus;
    private Atom                openOrPrepareWasCalled;
    // running time counter
    private UptimeComputer      uptime = new UptimeComputer();
    
    private StormClusterState   zkCluster;
    private Object              taskObj;
    private CommonStatsRolling  taskStats;
    
    @SuppressWarnings("rawtypes")
    public Task(WorkerData workerData, int taskId)
            throws Exception {
        openOrPrepareWasCalled = new Atom(Boolean.valueOf(false));
        
        this.topologyContext = workerData.getContextMaker()
                .makeTopologyContext(workerData.getSysTopology(), taskId, openOrPrepareWasCalled);
        this.userContext = workerData.getContextMaker()
                .makeTopologyContext(workerData.getRawTopology(), taskId, openOrPrepareWasCalled);
        
        this.taskTransfer = getSendingTransfer(workerData);
        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.topologyid = workerData.getTopologyId();
        this.context = workerData.getContext();
        this.workHalt = workerData.getWorkHalt();
        
        this.zkCluster = new StormZkClusterState(workerData.getZkClusterstate());
        
        
        
        this.taskid = taskId;
        this.componentid = topologyContext.getThisComponentId();
        this.stormConf = Common.component_conf(workerData.getStormConf(),
                topologyContext, componentid);
        String diskSlot = getAssignDiskSlot();
        JStormServerConfig.setTaskAssignDiskSlot(stormConf, diskSlot);
        
        this.taskStatus = new TaskStatus();
        
        // get real task object -- spout/bolt/spoutspec
        this.taskObj = Common.get_task_object(topologyContext.getRawTopology(),
                componentid);
        
        // get task statics object
        int samplerate = StormConfig.sampling_rate(stormConf);
        this.taskStats = new CommonStatsRolling(samplerate);
        
        LOG.info("Loading task " + componentid + ":" + taskid + " disk slot:" + diskSlot);
    }
    
    private TaskSendTargets makeSendTargets() {
        String component = topologyContext.getThisComponentId();
        
        // get current task's output
        // <Stream_id,<component, Grouping>>
        Map<String, Map<String, MkGrouper>> streamComponentGrouper = Common
                .outbound_components(topologyContext);
        
        Map<Integer, String> task2Component = topologyContext
                .getTaskToComponent();
        Map<String, List<Integer>> component2Tasks = JStormUtils
                .reverse_map(task2Component);
        
        return new TaskSendTargets(stormConf, component,
                streamComponentGrouper, topologyContext, component2Tasks,
                taskStats);
    }
    
    private String getAssignDiskSlot() {
        Assignment topologyAssignment = null;
        try {
            topologyAssignment = zkCluster.assignment_info(topologyid, null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get assignment ");
        }
        if (topologyAssignment == null) {
            throw new RuntimeException("Failed to get assignment ");
        }
        
        ResourceAssignment taskResource = topologyAssignment.getTaskToResource().get(taskid);
        if (taskResource == null) {
            throw new RuntimeException("Failed to get task ResourceAssignment of " + taskid);
        }
        
        return taskResource.getDiskSlot();
    }
    
    private TaskTransfer getSendingTransfer(WorkerData workerData) {


        // sending tuple's serializer
        KryoTupleSerializer serializer = new KryoTupleSerializer(
                workerData.getStormConf(), topologyContext);

        // Task sending all tuples through this Object
        return new TaskTransfer(serializer, workerData.getConf(), 
        		workerData.getTransferQueue(), 
        		workerData.getInnerTaskTransfer(),
        		workerData.getWorkHalt());
    }
    
    public TaskSendTargets echoToSystemBolt() {
        // send "startup" tuple to system bolt
        List<Object> msg = new ArrayList<Object>();
        msg.add("startup");
        
        // create task receive object
        TaskSendTargets sendTargets = makeSendTargets();
        UnanchoredSend.send(topologyContext, sendTargets, taskTransfer,
                Common.SYSTEM_STREAM_ID, msg);
        
        return sendTargets;
    }
    
    public RunnableCallback mk_executors(IConnection _puller,
            TaskSendTargets sendTargets, ITaskReportErr _report_error) {
        
        if (taskObj instanceof IBolt) {
            return new BoltExecutors((IBolt) taskObj, taskTransfer, 
            		innerTaskTransfer, stormConf, _puller, sendTargets, 
            		taskStatus, topologyContext, userContext, taskStats, _report_error);
        } else if (taskObj instanceof ISpout) {
            return new SpoutExecutors((ISpout) taskObj, taskTransfer, 
                    innerTaskTransfer, stormConf, _puller, sendTargets, 
                    taskStatus, topologyContext, userContext, taskStats, _report_error);
        }
        
        return null;
    }
    
    /**
     * create executor to receive tuples and run bolt/spout execute function
     * 
     * @param puller
     * @param sendTargets
     * @return
     */
    private RunnableCallback mkExecutor(IConnection puller,
            TaskSendTargets sendTargets) {
        // create report error callback, 
        // in fact it is storm_cluster.report-task-error
        ITaskReportErr reportError = new TaskReportError(zkCluster, topologyid,
                taskid);
        
        // report error and halt worker
        TaskReportErrorAndDie reportErrorDie = new TaskReportErrorAndDie(
                reportError, workHalt);
        
        return mk_executors(puller, sendTargets, reportErrorDie);
    }
    
    public TaskShutdownDameon execute() throws Exception {
        
        // create heartbeat
        TaskHeartbeatRunable hb = new TaskHeartbeatRunable(zkCluster,
                topologyid, taskid, uptime, taskStats, taskStatus, stormConf);
        
        AsyncLoopThread heartbeat_thread = new AsyncLoopThread(hb, false,
                Thread.MIN_PRIORITY, true);
        
        
        
        IConnection puller = context.bind(topologyid, taskid, false);
        
        TaskSendTargets sendTargets = echoToSystemBolt();
        
        // create thread to get tuple from zeroMQ,
        // and pass the tuple to bolt/spout 
        RunnableCallback baseExecutor = mkExecutor(puller, sendTargets);
        AsyncLoopThread executor_threads = new AsyncLoopThread(baseExecutor,
                false, null, Thread.MAX_PRIORITY, true);
        
        AsyncLoopThread[] all_threads = { executor_threads, heartbeat_thread };
        
        LOG.info("Finished loading task " + componentid + ":" + taskid);
        
        return getShutdown(all_threads, heartbeat_thread, puller);
    }
    
    public TaskShutdownDameon getShutdown(AsyncLoopThread[] all_threads,
            AsyncLoopThread heartbeat_thread, IConnection puller) {
        
        TaskShutdownDameon shutdown = new TaskShutdownDameon(taskStatus,
                topologyid, taskid, context, all_threads, zkCluster, puller,
                taskObj, heartbeat_thread);
        
        return shutdown;
    }
    
    public static TaskShutdownDameon mk_task(WorkerData workerData,
            int taskId) throws Exception {
        
        Task t = new Task(workerData,  taskId);
        
        return t.execute();
    }
    
}
