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
package com.alibaba.jstorm.task.execute.spout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.TimerRatio;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchFlushTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TimerConstants;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.TaskBatchTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.lmax.disruptor.EventHandler;

/**
 * spout executor
 * 
 * All spout actions will be done here
 * 
 * @author yannian/Longda
 * 
 */
public class SpoutExecutors extends BaseExecutors implements EventHandler {
    private static Logger LOG = LoggerFactory.getLogger(SpoutExecutors.class);

    protected final Integer max_spout_pending;

    protected backtype.storm.spout.ISpout spout;
    protected RotatingMap<Long, TupleInfo> pending;

    protected ISpoutOutputCollector output_collector;

    protected boolean firstTime = true;

    protected Histogram nextTupleTimer;
    protected Histogram ackerTimer;
    protected TimerRatio emptyCpuCounter;

    protected AsyncLoopThread ackerRunnableThread;

    protected boolean isSpoutFullSleep;

    public SpoutExecutors(Task task, backtype.storm.spout.ISpout _spout,
            TaskTransfer _transfer_fn,
            Map<Integer, DisruptorQueue> innerTaskTransfer, Map _storm_conf,
            TaskSendTargets sendTargets, TaskStatus taskStatus,
            TopologyContext topology_context, TopologyContext _user_context,
            TaskBaseMetric _task_stats, ITaskReportErr _report_error) {
        super(task, _transfer_fn, _storm_conf, innerTaskTransfer,
                topology_context, _user_context, _task_stats, taskStatus,
                _report_error);

        this.spout = _spout;

        this.max_spout_pending =
                JStormUtils.parseInt(storm_conf
                        .get(Config.TOPOLOGY_MAX_SPOUT_PENDING));

        this.nextTupleTimer =
                JStormMetrics.registerTaskHistogram(taskId,
                        MetricDef.EXECUTE_TIME);

        this.ackerTimer =
                JStormMetrics.registerTaskHistogram(taskId,
                        MetricDef.ACKER_TIME);

        this.emptyCpuCounter = new TimerRatio();
        JStormMetrics.registerTaskGauge(emptyCpuCounter, taskId,
                MetricDef.EMPTY_CPU_RATIO);

        isSpoutFullSleep = ConfigExtension.isSpoutPendFullSleep(storm_conf);

        if (ConfigExtension.isTaskBatchTuple(storm_conf)) {
            TaskBatchFlushTrigger batchFlushTrigger =
                    new TaskBatchFlushTrigger(5, idStr
                            + Constants.SYSTEM_COMPONENT_ID,
                            (TaskBatchTransfer) _transfer_fn);
            batchFlushTrigger.register(TimeUnit.MILLISECONDS);
        }

        LOG.info("isSpoutFullSleep:" + isSpoutFullSleep);

    }

    public void prepare(TaskSendTargets sendTargets, TaskTransfer transferFn,
            TopologyContext topologyContext) {

        JStormMetrics.registerTaskGauge(
                new com.codahale.metrics.Gauge<Double>() {

                    @Override
                    public Double getValue() {
                        return (double) pending.size();
                    }

                }, taskId, MetricDef.PENDING_MAP);

        // collector, in fact it call send_spout_msg
        this.output_collector =
                new SpoutCollector(taskId, spout, task_stats, sendTargets,
                        storm_conf, transferFn, pending, topologyContext,
                        exeQueue, report_error);

        try {
            WorkerClassLoader.switchThreadContext();
            this.spout.open(storm_conf, userTopologyCtx,
                    new SpoutOutputCollector(output_collector));
        } catch (Throwable e) {
            error = e;
            LOG.error("spout open error ", e);
            report_error.report(e);
        } finally {
            WorkerClassLoader.restoreThreadContext();
        }

        LOG.info("Successfully create SpoutExecutors " + idStr);

    }

    public void nextTuple() {
        if (firstTime == true) {

            int delayRun = ConfigExtension.getSpoutDelayRunSeconds(storm_conf);

            // wait other bolt is ready
            JStormUtils.sleepMs(delayRun * 1000);

            emptyCpuCounter.init();

            if (taskStatus.isRun() == true) {
                spout.activate();
            } else {
                spout.deactivate();
            }

            firstTime = false;
            LOG.info(idStr + " is ready ");
        }

        if (taskStatus.isRun() == false) {
            JStormUtils.sleepMs(1);
            return;
        }

        // if don't need ack, pending map will be always empty
        if (max_spout_pending == null || pending.size() < max_spout_pending) {
            emptyCpuCounter.stop();

            long start = System.nanoTime();
            try {
                spout.nextTuple();
            } catch (Throwable e) {
                error = e;
                LOG.error("spout execute error ", e);
                report_error.report(e);
            } finally {
                long end = System.nanoTime();
                nextTupleTimer.update((end - start) / 1000000.0d);
            }

            return;
        } else {
            if (isSpoutFullSleep) {
                JStormUtils.sleepMs(1);
            }
            emptyCpuCounter.start();
            // just return, no sleep
        }
    }

    @Override
    public void run() {

        throw new RuntimeException("Should implement this function");
    }

    /**
     * Handle acker message
     * 
     * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long,
     *      boolean)
     */
    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch)
            throws Exception {
        long start = System.nanoTime();
        try {

            if (event == null) {
                return;
            }

            Runnable runnable = null;
            if (event instanceof Tuple) {
                runnable = processTupleEvent((Tuple) event);
            } else if (event instanceof BatchTuple) {
                for (Tuple tuple : ((BatchTuple) event).getTuples()) {
                    runnable = processTupleEvent(tuple);
                    if (runnable != null) {
                        runnable.run();
                        runnable = null;
                    }
                }
            } else if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
                return;
            } else if (event instanceof IAckMsg) {

                runnable = (Runnable) event;
            } else if (event instanceof Runnable) {

                runnable = (Runnable) event;
            } else {

                LOG.warn("Receive one unknow event-" + event.toString() + " "
                        + idStr);
                return;
            }

            if (runnable != null)
                runnable.run();

        } catch (Throwable e) {
            if (taskStatus.isShutdown() == false) {
                LOG.info("Unknow excpetion ", e);
                report_error.report(e);
            }
        } finally {
            long end = System.nanoTime();
            ackerTimer.update((end - start) / 1000000.0d);
        }
    }

    private Runnable processTupleEvent(Tuple event) {
        Runnable runnable;
        Tuple tuple = (Tuple) event;
        Object id = tuple.getValue(0);
        Object obj = pending.remove((Long) id);

        if (obj == null) {
            if (isDebug) {
                LOG.info("Pending map no entry:" + id);
            }
            runnable = null;
        } else {
            TupleInfo tupleInfo = (TupleInfo) obj;

            String stream_id = tuple.getSourceStreamId();

            if (stream_id.equals(Acker.ACKER_ACK_STREAM_ID)) {

                runnable =
                        new AckSpoutMsg(spout, tupleInfo, task_stats, isDebug);
            } else if (stream_id.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                runnable =
                        new FailSpoutMsg(id, spout, tupleInfo, task_stats,
                                isDebug);
            } else {
                LOG.warn("Receive one unknow source Tuple " + idStr);
                runnable = null;
            }
        }

        task_stats.recv_tuple(tuple.getSourceComponent(),
                tuple.getSourceStreamId());
        return runnable;
    }

    public AsyncLoopThread getAckerRunnableThread() {
        return ackerRunnableThread;
    }

    private void processTimerEvent(TimerTrigger.TimerEvent event) {
        switch (event.getOpCode()) {
        case TimerConstants.ROTATING_MAP: {
            Map<Long, TupleInfo> timeoutMap = pending.rotate();
            for (java.util.Map.Entry<Long, TupleInfo> entry : timeoutMap
                    .entrySet()) {
                TupleInfo tupleInfo = entry.getValue();
                FailSpoutMsg fail =
                        new FailSpoutMsg(entry.getKey(), spout,
                                (TupleInfo) tupleInfo, task_stats, isDebug);
                fail.run();
            }
            break;
        }
        case TimerConstants.TASK_HEARTBEAT: {
            Integer taskId = (Integer) event.getMsg();
            TaskHeartbeatRunable.updateTaskHbStats(taskId, task);
            break;
        }
        default: {
            LOG.warn("Receive unsupported timer event, opcode="
                    + event.getOpCode());
            break;
        }
        }
    }

    protected void processControlEvent() {
        Object event = controlQueue.poll();

        if (event != null) {
            if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
            } else {
                LOG.warn("Received unknown control event, "
                        + event.getClass().getName());
            }
        }
    }
}
