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
package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import clojure.lang.Atom;

import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.execute.spout.MultipleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SingleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SpoutExecutors;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.task.heartbeat.TaskStats;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Task instance
 * 
 * @author yannian/Longda
 * 
 */
public class Task {

    private final static Logger LOG = LoggerFactory.getLogger(Task.class);

    private Map<Object, Object> stormConf;

    private TopologyContext topologyContext;
    private TopologyContext userContext;
    private String topologyid;
    private IContext context;

    private TaskTransfer taskTransfer;
    private TaskReceiver taskReceiver;
    private Map<Integer, DisruptorQueue> innerTaskTransfer;
    private Map<Integer, DisruptorQueue> deserializeQueues;
    private AsyncLoopDefaultKill workHalt;

    private Integer taskid;
    private String componentid;
    private volatile TaskStatus taskStatus;
    private Atom openOrPrepareWasCalled;
    // running time counter
    private UptimeComputer uptime = new UptimeComputer();

    private StormClusterState zkCluster;
    private Object taskObj;
    private TaskBaseMetric taskStats;
    private WorkerData workerData;
    private String componentType; // "spout" or "bolt"

    private TaskSendTargets taskSendTargets;

    private boolean isTaskBatchTuple;

    @SuppressWarnings("rawtypes")
    public Task(WorkerData workerData, int taskId) throws Exception {
        openOrPrepareWasCalled = new Atom(Boolean.valueOf(false));

        this.workerData = workerData;
        this.topologyContext =
                workerData.getContextMaker().makeTopologyContext(
                        workerData.getSysTopology(), taskId,
                        openOrPrepareWasCalled);
        this.userContext =
                workerData.getContextMaker().makeTopologyContext(
                        workerData.getRawTopology(), taskId,
                        openOrPrepareWasCalled);
        this.taskid = taskId;
        this.componentid = topologyContext.getThisComponentId();
        this.stormConf =
                Common.component_conf(workerData.getStormConf(),
                        topologyContext, componentid);

        this.taskStatus = new TaskStatus();
        this.taskStats = new TaskBaseMetric(taskId);

        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.deserializeQueues = workerData.getDeserializeQueues();
        this.topologyid = workerData.getTopologyId();
        this.context = workerData.getContext();
        this.workHalt = workerData.getWorkHalt();
        this.zkCluster =
                new StormZkClusterState(workerData.getZkClusterstate());

        LOG.info("Begin to deserialize taskObj " + componentid + ":" + taskid);

        WorkerClassLoader.switchThreadContext();
        // get real task object -- spout/bolt/spoutspec
        this.taskObj =
                Common.get_task_object(topologyContext.getRawTopology(),
                        componentid, WorkerClassLoader.getInstance());
        WorkerClassLoader.restoreThreadContext();

        isTaskBatchTuple = ConfigExtension.isTaskBatchTuple(stormConf);
        LOG.info("Transfer/receive in batch mode :" + isTaskBatchTuple);

        LOG.info("Loading task " + componentid + ":" + taskid);
    }

    private void setComponentType() {
        if (taskObj instanceof IBolt) {
            componentType = "bolt";
        } else if (taskObj instanceof ISpout) {
            componentType = "spout";
        }
    }

    private TaskSendTargets makeSendTargets() {
        String component = topologyContext.getThisComponentId();

        // get current task's output
        // <Stream_id,<component, Grouping>>
        Map<String, Map<String, MkGrouper>> streamComponentGrouper =
                Common.outbound_components(topologyContext, workerData);

        return new TaskSendTargets(stormConf, component,
                streamComponentGrouper, topologyContext, taskStats);
    }

    private void updateSendTargets() {
        if (taskSendTargets != null) {
            Map<String, Map<String, MkGrouper>> streamComponentGrouper =
                    Common.outbound_components(topologyContext, workerData);
            taskSendTargets.updateStreamCompGrouper(streamComponentGrouper);
        } else {
            LOG.error("taskSendTargets is null when trying to update it.");
        }
    }

    private TaskTransfer mkTaskSending(WorkerData workerData) {

        // sending tuple's serializer
        KryoTupleSerializer serializer =
                new KryoTupleSerializer(workerData.getStormConf(),
                        topologyContext);

        String taskName = JStormServerUtils.getName(componentid, taskid);
        // Task sending all tuples through this Object
        TaskTransfer taskTransfer;
        if (isTaskBatchTuple)
            taskTransfer =
                    new TaskBatchTransfer(this, taskName, serializer,
                            taskStatus, workerData);
        else
            taskTransfer =
                    new TaskTransfer(this, taskName, serializer, taskStatus,
                            workerData);
        return taskTransfer;
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

    public boolean isSingleThread(Map conf) {
        boolean isOnePending = JStormServerUtils.isOnePending(conf);
        if (isOnePending == true) {
            return true;
        }

        return ConfigExtension.isSpoutSingleThread(conf);
    }

    public RunnableCallback mk_executors(TaskSendTargets sendTargets,
            ITaskReportErr report_error) {

        if (taskObj instanceof IBolt) {
            return new BoltExecutors(this, (IBolt) taskObj, taskTransfer,
                    innerTaskTransfer, stormConf, sendTargets, taskStatus,
                    topologyContext, userContext, taskStats, report_error);
        } else if (taskObj instanceof ISpout) {
            if (isSingleThread(stormConf) == true) {
                return new SingleThreadSpoutExecutors(this, (ISpout) taskObj,
                        taskTransfer, innerTaskTransfer, stormConf,
                        sendTargets, taskStatus, topologyContext, userContext,
                        taskStats, report_error);
            } else {
                return new MultipleThreadSpoutExecutors(this, (ISpout) taskObj,
                        taskTransfer, innerTaskTransfer, stormConf,
                        sendTargets, taskStatus, topologyContext, userContext,
                        taskStats, report_error);
            }
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
    private RunnableCallback mkExecutor(TaskSendTargets sendTargets) {
        // create report error callback,
        // in fact it is storm_cluster.report-task-error
        ITaskReportErr reportError =
                new TaskReportError(zkCluster, topologyid, taskid);

        // report error and halt worker
        TaskReportErrorAndDie reportErrorDie =
                new TaskReportErrorAndDie(reportError, workHalt);

        return mk_executors(sendTargets, reportErrorDie);
    }

    public TaskReceiver mkTaskReceiver() {
        String taskName = JStormServerUtils.getName(componentid, taskid);
        TaskReceiver taskReceiver;
        if (isTaskBatchTuple)
            taskReceiver =
                    new TaskBatchReceiver(this, taskid, stormConf,
                            topologyContext, innerTaskTransfer, taskStatus,
                            taskName);
        else
            taskReceiver =
                    new TaskReceiver(this, taskid, stormConf, topologyContext,
                            innerTaskTransfer, taskStatus, taskName);
        deserializeQueues.put(taskid, taskReceiver.getDeserializeQueue());
        return taskReceiver;
    }

    public TaskShutdownDameon execute() throws Exception {
        setComponentType();

        taskSendTargets = echoToSystemBolt();

        // create thread to get tuple from zeroMQ,
        // and pass the tuple to bolt/spout
        taskTransfer = mkTaskSending(workerData);
        RunnableCallback baseExecutor = mkExecutor(taskSendTargets);
        AsyncLoopThread executor_threads =
                new AsyncLoopThread(baseExecutor, false, Thread.MAX_PRIORITY,
                        true);
        taskReceiver = mkTaskReceiver();

        List<AsyncLoopThread> allThreads = new ArrayList<AsyncLoopThread>();
        allThreads.add(executor_threads);

        TaskHeartbeatRunable.registerTaskStats(taskid, new TaskStats(
                componentType, taskStats));
        LOG.info("Finished loading task " + componentid + ":" + taskid);

        return getShutdown(allThreads, taskReceiver.getDeserializeQueue(),
                baseExecutor);
    }

    public TaskShutdownDameon getShutdown(List<AsyncLoopThread> allThreads,
            DisruptorQueue deserializeQueue, RunnableCallback baseExecutor) {

        AsyncLoopThread ackerThread = null;
        if (baseExecutor instanceof SpoutExecutors) {
            ackerThread =
                    ((SpoutExecutors) baseExecutor).getAckerRunnableThread();

            if (ackerThread != null) {
                allThreads.add(ackerThread);
            }
        }
        AsyncLoopThread recvThread = taskReceiver.getDeserializeThread();
        allThreads.add(recvThread);

        AsyncLoopThread serializeThread = taskTransfer.getSerializeThread();
        allThreads.add(serializeThread);

        TaskShutdownDameon shutdown =
                new TaskShutdownDameon(taskStatus, topologyid, taskid,
                        allThreads, zkCluster, taskObj);

        return shutdown;
    }

    public static TaskShutdownDameon mk_task(WorkerData workerData, int taskId)
            throws Exception {

        Task t = new Task(workerData, taskId);

        return t.execute();
    }

    /**
     * Update the data which can be changed dynamically e.g. when scale-out of a
     * task parallelism
     */
    public void updateTaskData() {
        // Only update the local task list of topologyContext here. Because
        // other
        // relative parts in context shall be updated while the updating of
        // WorkerData (Task2Component and Component2Task map)
        List<Integer> localTasks = JStormUtils.mk_list(workerData.getTaskids());
        topologyContext.setThisWorkerTasks(localTasks);
        userContext.setThisWorkerTasks(localTasks);

        // Update the TaskSendTargets
        updateSendTargets();
    }

    public long getWorkerAssignmentTs() {
        return workerData.getAssignmentTs();
    }
    
    public AssignmentType getWorkerAssignmentType() {
        return workerData.getAssignmentType();
    }

    public void unregisterDeserializeQueue() {
        deserializeQueues.remove(taskid);
    }
}
