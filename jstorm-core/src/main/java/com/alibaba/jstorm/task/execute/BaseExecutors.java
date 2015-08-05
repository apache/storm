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
package com.alibaba.jstorm.task.execute;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

//import com.alibaba.jstorm.message.zeroMq.IRecvConnection;

/**
 * Base executor share between spout and bolt
 * 
 * 
 * @author Longda
 * 
 */
public class BaseExecutors extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(BaseExecutors.class);

    protected final String component_id;
    protected final int taskId;
    protected final String idStr;

    protected Map storm_conf;
    
    protected final boolean isDebug;

    protected TopologyContext userTopologyCtx;
    protected TaskBaseMetric task_stats;

    protected volatile TaskStatus taskStatus;

    protected int message_timeout_secs = 30;

    protected Throwable error = null;

    protected ITaskReportErr report_error;

    protected DisruptorQueue exeQueue;
    protected BlockingQueue<Object> controlQueue;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;

    protected Task task;
    protected long assignmentTs;
    protected TaskTransfer taskTransfer;

    // protected IntervalCheck intervalCheck = new IntervalCheck();

    public BaseExecutors(Task task, TaskTransfer _transfer_fn, Map _storm_conf,
            Map<Integer, DisruptorQueue> innerTaskTransfer,
            TopologyContext topology_context, TopologyContext _user_context,
            TaskBaseMetric _task_stats, TaskStatus taskStatus,
            ITaskReportErr _report_error) {

        this.task = task;
        this.storm_conf = _storm_conf;

        this.userTopologyCtx = _user_context;
        this.task_stats = _task_stats;
        this.taskId = topology_context.getThisTaskId();
        this.innerTaskTransfer = innerTaskTransfer;
        this.component_id = topology_context.getThisComponentId();
        this.idStr = JStormServerUtils.getName(component_id, taskId);

        this.taskStatus = taskStatus;
        this.report_error = _report_error;

        this.isDebug =
                JStormUtils.parseBoolean(storm_conf.get(Config.TOPOLOGY_DEBUG),
                        false);

        message_timeout_secs =
                JStormUtils.parseInt(
                        storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS),
                        30);

        int queue_size =
                Utils.getInt(storm_conf
                        .get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);
        WaitStrategy waitStrategy =
                (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        this.exeQueue =
                DisruptorQueue.mkInstance(idStr, ProducerType.MULTI,
                        queue_size, waitStrategy);
        this.exeQueue.consumerStarted();
        this.controlQueue = new LinkedBlockingDeque<Object>(8);

        this.registerInnerTransfer(exeQueue);

        QueueGauge exeQueueGauge =
                new QueueGauge(idStr + MetricDef.EXECUTE_QUEUE, exeQueue);
        JStormMetrics.registerTaskGauge(exeQueueGauge, taskId,
                MetricDef.EXECUTE_QUEUE);
        JStormHealthCheck.registerTaskHealthCheck(taskId,
                MetricDef.EXECUTE_QUEUE, exeQueueGauge);

        RotatingMapTrigger rotatingMapTrigger =
                new RotatingMapTrigger(storm_conf, idStr + "_rotating",
                        exeQueue);
        rotatingMapTrigger.register();
        TaskHeartbeatTrigger taskHbTrigger =
                new TaskHeartbeatTrigger(storm_conf, idStr + "_taskHeartbeat",
                        exeQueue, controlQueue, taskId);
        taskHbTrigger.register();

        assignmentTs = System.currentTimeMillis();
        
        this.taskTransfer = _transfer_fn;
    }

    @Override
    public void preRun() {
        WorkerClassLoader.switchThreadContext();  
    }

    @Override
    public void postRun() {
        WorkerClassLoader.restoreThreadContext();
    }

    @Override
    public void run() {
        // this function will be override by SpoutExecutor or BoltExecutor
        throw new RuntimeException("Should implement this function");
    }

    // @Override
    // public Object getResult() {
    // if (taskStatus.isRun()) {
    // return 0;
    // } else if (taskStatus.isPause()) {
    // return 0;
    // } else if (taskStatus.isShutdown()) {
    // this.shutdown();
    // return -1;
    // } else {
    // LOG.info("Unknow TaskStatus, shutdown executing thread of " + idStr);
    // this.shutdown();
    // return -1;
    // }
    // }

    @Override
    public Exception error() {
        if (error == null) {
            return null;
        }

        return new Exception(error);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutdown executing thread of " + idStr);
        if (taskStatus.isShutdown() == false) {
            LOG.info("Taskstatus isn't shutdown, but enter shutdown method, Occur exception");
        }
        this.unregistorInnerTransfer();

    }

    protected void registerInnerTransfer(DisruptorQueue disruptorQueue) {
        LOG.info("Registor inner transfer for executor thread of " + idStr);
        DisruptorQueue existInnerTransfer = innerTaskTransfer.get(taskId);
        if (existInnerTransfer != null) {
            LOG.info("Exist inner task transfer for executing thread of "
                    + idStr);
            if (existInnerTransfer != disruptorQueue) {
                throw new RuntimeException(
                        "Inner task transfer must be only one in executing thread of "
                                + idStr);
            }
        }
        innerTaskTransfer.put(taskId, disruptorQueue);
    }

    protected void unregistorInnerTransfer() {
        LOG.info("Unregistor inner transfer for executor thread of " + idStr);
        innerTaskTransfer.remove(taskId);
    }

}
