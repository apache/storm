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

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchFlushTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskBatchTransfer;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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

    protected final String topologyId;
    protected final String componentId;
    protected final int taskId;
    protected final String idStr;

    protected Map storm_conf;

    protected final boolean isDebug;

    protected TopologyContext userTopologyCtx;
    protected TopologyContext sysTopologyCtx;
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
   
    protected JStormMetricsReporter metricsReporter;
    
    protected boolean isFinishInit = false;

    protected RotatingMapTrigger rotatingMapTrigger;
    protected TaskHeartbeatTrigger taskHbTrigger;

    // protected IntervalCheck intervalCheck = new IntervalCheck();

    public BaseExecutors(Task task) {

        this.task = task;
        this.storm_conf = task.getStormConf();

        this.userTopologyCtx = task.getUserContext();
        this.sysTopologyCtx = task.getTopologyContext();
        this.task_stats = task.getTaskStats();
        this.taskId = sysTopologyCtx.getThisTaskId();
        this.innerTaskTransfer = task.getInnerTaskTransfer();
        this.topologyId = sysTopologyCtx.getTopologyId();
        this.componentId = sysTopologyCtx.getThisComponentId();
        this.idStr = JStormServerUtils.getName(componentId, taskId);

        this.taskStatus = task.getTaskStatus();
        this.report_error = task.getReportErrorDie();
        this.taskTransfer = task.getTaskTransfer();
        this.metricsReporter = task.getWorkerData().getMetricsReporter();

        this.isDebug = JStormUtils.parseBoolean(storm_conf.get(Config.TOPOLOGY_DEBUG), false);

        message_timeout_secs = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);
        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        this.exeQueue = DisruptorQueue.mkInstance(idStr, ProducerType.MULTI, queue_size, waitStrategy);
        this.exeQueue.consumerStarted();
        this.controlQueue = new LinkedBlockingDeque<Object>();

        this.registerInnerTransfer(exeQueue);

        QueueGauge exeQueueGauge = new QueueGauge(exeQueue, idStr, MetricDef.EXECUTE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, componentId, taskId, MetricDef.EXECUTE_QUEUE, MetricType.GAUGE), new AsmGauge(
                exeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.EXECUTE_QUEUE, exeQueueGauge);

        rotatingMapTrigger = new RotatingMapTrigger(storm_conf, idStr + "_rotating", exeQueue);
        rotatingMapTrigger.register();
        taskHbTrigger = new TaskHeartbeatTrigger(storm_conf, idStr + "_taskHeartbeat", exeQueue, controlQueue, taskId, componentId, sysTopologyCtx, report_error);
        taskHbTrigger.register();
        
        assignmentTs = System.currentTimeMillis();
        
    }
    
    public void init() throws Exception {
    	// this function will be override by SpoutExecutor or BoltExecutor
        throw new RuntimeException("Should implement this function");
    }
    
    public void initWrapper() {
    	try {
            LOG.info("{} begin to init", idStr);
            
            init();
            
            if (taskId == getMinTaskIdOfWorker()) {
                metricsReporter.setOutputCollector(getOutputCollector());
            }
            
            isFinishInit = true;
        } catch (Throwable e) {
            error = e;
            LOG.error("Init error ", e);
            report_error.report(e);
        } finally {

            LOG.info("{} initialization finished", idStr);
            
        }
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
            LOG.info("Exist inner task transfer for executing thread of " + idStr);
            if (existInnerTransfer != disruptorQueue) {
                throw new RuntimeException("Inner task transfer must be only one in executing thread of " + idStr);
            }
        }
        innerTaskTransfer.put(taskId, disruptorQueue);
    }

    protected void unregistorInnerTransfer() {
        LOG.info("Unregistor inner transfer for executor thread of " + idStr);
        innerTaskTransfer.remove(taskId);
    }

    protected int getMinTaskIdOfWorker() {
        SortedSet<Integer> tasks = new TreeSet<Integer>(sysTopologyCtx.getThisWorkerTasks());
        return tasks.first();
    }
    
    public Object getOutputCollector() {
    	return null;
    }
}
