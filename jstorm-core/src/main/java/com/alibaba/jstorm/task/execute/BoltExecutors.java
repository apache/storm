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
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchFlushTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TickTupleTrigger;
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
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.alibaba.jstorm.utils.TimeUtils;
import com.lmax.disruptor.EventHandler;

/**
 * 
 * BoltExecutor
 * 
 * @author yannian/Longda
 * 
 */
public class BoltExecutors extends BaseExecutors implements EventHandler {
    private static Logger LOG = LoggerFactory.getLogger(BoltExecutors.class);

    protected IBolt bolt;

    protected RotatingMap<Tuple, Long> tuple_start_times;

    private int ackerNum = 0;

    // internal outputCollector is BoltCollector
    private OutputCollector outputCollector;

    private Histogram boltExeTimer;

    public BoltExecutors(Task task, IBolt _bolt, TaskTransfer _transfer_fn,
            Map<Integer, DisruptorQueue> innerTaskTransfer, Map storm_conf,
            TaskSendTargets _send_fn, TaskStatus taskStatus,
            TopologyContext sysTopologyCxt, TopologyContext userTopologyCxt,
            TaskBaseMetric _task_stats, ITaskReportErr _report_error) {

        super(task, _transfer_fn, storm_conf, innerTaskTransfer,
                sysTopologyCxt, userTopologyCxt, _task_stats, taskStatus,
                _report_error);

        this.bolt = _bolt;

        // create TimeCacheMap

        this.tuple_start_times =
                new RotatingMap<Tuple, Long>(Acker.TIMEOUT_BUCKET_NUM);

        this.ackerNum =
                JStormUtils.parseInt(storm_conf
                        .get(Config.TOPOLOGY_ACKER_EXECUTORS));

        // don't use TimeoutQueue for recv_tuple_queue,
        // then other place should check the queue size
        // TimeCacheQueue.DefaultExpiredCallback<Tuple> logExpireCb = new
        // TimeCacheQueue.DefaultExpiredCallback<Tuple>(
        // idStr);
        // this.recv_tuple_queue = new
        // TimeCacheQueue<Tuple>(message_timeout_secs,
        // TimeCacheQueue.DEFAULT_NUM_BUCKETS, logExpireCb);

        // create BoltCollector
        IOutputCollector output_collector =
                new BoltCollector(message_timeout_secs, _report_error,
                        _send_fn, storm_conf, _transfer_fn, sysTopologyCxt,
                        taskId, tuple_start_times, _task_stats);

        outputCollector = new OutputCollector(output_collector);

        boltExeTimer =
                JStormMetrics.registerTaskHistogram(taskId,
                        MetricDef.EXECUTE_TIME);

        Object tickFrequence =
                storm_conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        if (tickFrequence != null) {
            Integer frequence = JStormUtils.parseInt(tickFrequence);
            TickTupleTrigger tickTupleTrigger =
                    new TickTupleTrigger(sysTopologyCxt, frequence, idStr
                            + Constants.SYSTEM_TICK_STREAM_ID, exeQueue);
            tickTupleTrigger.register();
        }

        if (ConfigExtension.isTaskBatchTuple(storm_conf)) {
            TaskBatchFlushTrigger batchFlushTrigger =
                    new TaskBatchFlushTrigger(5, idStr
                            + Constants.SYSTEM_COMPONENT_ID,
                            (TaskBatchTransfer) _transfer_fn);
            batchFlushTrigger.register(TimeUnit.MILLISECONDS);
        }

        try {
            // do prepare
            WorkerClassLoader.switchThreadContext();

            // Method method = IBolt.class.getMethod("prepare", new Class[]
            // {Map.class, TopologyContext.class,
            // OutputCollector.class});
            // method.invoke(bolt, new Object[] {storm_conf, userTopologyCxt,
            // outputCollector});
            bolt.prepare(storm_conf, userTopologyCtx, outputCollector);

        } catch (Throwable e) {
            error = e;
            LOG.error("bolt prepare error ", e);
            report_error.report(e);
        } finally {
            WorkerClassLoader.restoreThreadContext();
        }

        LOG.info("Successfully create BoltExecutors " + idStr);

    }

    @Override
    public String getThreadName() {
        return idStr + "-" + BoltExecutors.class.getSimpleName();
    }

    @Override
    public void run() {
        while (taskStatus.isShutdown() == false) {
            try {
                exeQueue.consumeBatchWhenAvailable(this);

                processControlEvent();
            } catch (Throwable e) {
                if (taskStatus.isShutdown() == false) {
                    LOG.error(idStr + " bolt exeutor  error", e);
                }
            }
        }
    }

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch)
            throws Exception {

        if (event == null) {
            return;
        }

        long start = System.nanoTime();

        try {
            if (event instanceof Tuple) {
                processTupleEvent((Tuple) event);
            } else if (event instanceof BatchTuple) {
                for (Tuple tuple : ((BatchTuple) event).getTuples()) {
                    processTupleEvent((Tuple) tuple);
                }
            } else if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
            } else {
                LOG.warn("Bolt executor received unknown message");
            }
        } finally {
            long end = System.nanoTime();
            boltExeTimer.update((end - start) / 1000000.0d);
        }
    }

    private void processTupleEvent(Tuple tuple) {
        task_stats.recv_tuple(tuple.getSourceComponent(),
                tuple.getSourceStreamId());

        tuple_start_times.put(tuple, System.currentTimeMillis());

        try {
            bolt.execute(tuple);
        } catch (Throwable e) {
            error = e;
            LOG.error("bolt execute error ", e);
            report_error.report(e);
        }

        if (ackerNum == 0) {
            // only when acker is disable
            // get tuple process latency
            Long start_time = (Long) tuple_start_times.remove(tuple);
            if (start_time != null) {
                Long delta = TimeUtils.time_delta_ms(start_time);
                task_stats.bolt_acked_tuple(tuple.getSourceComponent(),
                        tuple.getSourceStreamId(), Double.valueOf(delta));
            }
        }
    }

    private void processTimerEvent(TimerTrigger.TimerEvent event) {
        switch (event.getOpCode()) {
        case TimerConstants.ROTATING_MAP: {
            Map<Tuple, Long> timeoutMap = tuple_start_times.rotate();

            if (ackerNum > 0) {
                // only when acker is enable
                for (Entry<Tuple, Long> entry : timeoutMap.entrySet()) {
                    Tuple input = entry.getKey();
                    task_stats.bolt_failed_tuple(input.getSourceComponent(),
                            input.getSourceStreamId());
                }
            }
            break;
        }
        case TimerConstants.TICK_TUPLE: {
            try {
                Tuple tuple = (Tuple) event.getMsg();
                bolt.execute(tuple);
            } catch (Throwable e) {
                error = e;
                LOG.error("bolt execute error ", e);
                report_error.report(e);
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
                LOG.debug("Received one event from control queue");
            } else {
                LOG.warn("Received unknown control event, "
                        + event.getClass().getName());
            }
        }
    }
}
