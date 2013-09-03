package com.alipay.dw.jstorm.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.client.ConfigExtension;
import com.alipay.dw.jstorm.cluster.ClusterState;
import com.alipay.dw.jstorm.cluster.Common;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.cluster.StormZkClusterState;
import com.alipay.dw.jstorm.daemon.worker.WorkerData;
import com.alipay.dw.jstorm.daemon.worker.WorkerHaltRunable;
import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.comm.TaskSendTargets;
import com.alipay.dw.jstorm.task.comm.UnanchoredSend;
import com.alipay.dw.jstorm.task.error.ITaskReportErr;
import com.alipay.dw.jstorm.task.error.TaskReportError;
import com.alipay.dw.jstorm.task.error.TaskReportErrorAndDie;
import com.alipay.dw.jstorm.task.execute.BoltExecutors;
import com.alipay.dw.jstorm.task.execute.SpoutExecutors;
import com.alipay.dw.jstorm.task.group.MkGrouper;
import com.alipay.dw.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

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
    private MQContext           mqContext;
    
    private WorkerTransfer      workerTransfer;
    private WorkerHaltRunable   workHalt;
    
    private Integer             taskid;
    private String              componentid;
    private TaskStatus          taskStatus;
    // running time counter
    private UptimeComputer      uptime = new UptimeComputer();
    
    private StormClusterState   zkCluster;
    private Object              taskObj;
    private CommonStatsRolling  taskStats;
    
    @SuppressWarnings("rawtypes")
    public Task(WorkerData workerData, TopologyContext topologyContext,
            TopologyContext userContext, WorkerTransfer workerTransfer)
            throws Exception {
        
        this.topologyContext = topologyContext;
        this.userContext = userContext;
        this.workerTransfer = workerTransfer;
        this.topologyid = workerData.getTopologyId();
        this.mqContext = workerData.getMqContext();
        this.workHalt = workerData.getWorkHalt();
        
        this.taskid = topologyContext.getThisTaskId();
        this.componentid = topologyContext.getThisComponentId();
        this.stormConf = Common.component_conf(workerData.getStormConf(),
                topologyContext, componentid);
        
        this.taskStatus = new TaskStatus();
        
        this.zkCluster = new StormZkClusterState(workerData.getZkClusterstate());
        
        // get real task object -- spout/bolt/spoutspec
        this.taskObj = Common.get_task_object(topologyContext.getRawTopology(),
                componentid);
        
        // get task statics object
        int samplerate = StormConfig.sampling_rate(stormConf);
        this.taskStats = new CommonStatsRolling(samplerate);
        
        LOG.info("Loading task " + componentid + ":" + taskid);
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
    
    public TaskSendTargets echoToSystemBolt() {
        // send "startup" tuple to system bolt
        List<Object> msg = new ArrayList<Object>();
        msg.add("startup");
        
        // create task receive object
        TaskSendTargets sendTargets = makeSendTargets();
        UnanchoredSend.send(topologyContext, sendTargets, workerTransfer,
                Common.SYSTEM_STREAM_ID, msg);
        
        return sendTargets;
    }
    
    public RunnableCallback mk_executors(IRecvConnection _puller,
            TaskSendTargets sendTargets, ITaskReportErr _report_error) {
        
        if (taskObj instanceof IBolt) {
            return new BoltExecutors((IBolt) taskObj, workerTransfer,
                    stormConf, _puller, sendTargets, taskStatus,
                    topologyContext, userContext, taskStats, _report_error);
        } else if (taskObj instanceof ISpout) {
            return new SpoutExecutors((ISpout) taskObj, workerTransfer,
                    stormConf, _puller, sendTargets, taskStatus,
                    topologyContext, userContext, taskStats, _report_error);
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
    private RunnableCallback mkExecutor(IRecvConnection puller,
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
        
        IRecvConnection puller = mqContext.bind(false, taskid);
        
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
            AsyncLoopThread heartbeat_thread, IRecvConnection puller) {
        
        TaskShutdownDameon shutdown = new TaskShutdownDameon(taskStatus,
                topologyid, taskid, mqContext, all_threads, zkCluster, puller,
                taskObj, heartbeat_thread);
        
        return shutdown;
    }
    
    public static TaskShutdownDameon mk_task(WorkerData workerData,
            WorkerTransfer workerTransfer, int taskId) throws Exception {
        
        TopologyContext topologyContext = workerData.getContextMaker()
                .makeTopologyContext(workerData.getSysTopology(), taskId);
        TopologyContext userContext = workerData.getContextMaker()
                .makeTopologyContext(workerData.getRawTopology(), taskId);
        
        Task t = new Task(workerData, topologyContext, userContext,
                workerTransfer);
        
        return t.execute();
    }
    
}
