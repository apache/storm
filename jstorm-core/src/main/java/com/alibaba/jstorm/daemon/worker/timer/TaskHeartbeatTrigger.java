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
package com.alibaba.jstorm.daemon.worker.timer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.UptimeComputer;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class TaskHeartbeatTrigger extends TimerTrigger {
    private static final Logger LOG = LoggerFactory.getLogger(TaskHeartbeatTrigger.class);

    private int taskId;
    private String componentId;
    private TopologyContext sysTopologyCtx;

    private BlockingQueue<Object> controlQueue = null;

    private OutputCollector boltOutputCollector = null;
    private SpoutOutputCollector spoutOutputCollector = null;

    private long executeThreadHbTime;
    private int taskHbTimeout;

    private ITaskReportErr reportError;

    private IntervalCheck intervalCheck;

    private UptimeComputer uptime;

    public TaskHeartbeatTrigger(Map conf, String name, DisruptorQueue queue, BlockingQueue<Object> controlQueue, int taskId, String componentId,
            TopologyContext sysTopologyCtx, ITaskReportErr reportError) {
        this.name = name;
        this.queue = queue;
        this.controlQueue = controlQueue;
        this.opCode = TimerConstants.TASK_HEARTBEAT;

        this.taskId = taskId;
        this.componentId = componentId;
        this.sysTopologyCtx = sysTopologyCtx;

        this.frequence = JStormUtils.parseInt(conf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 10);
        this.firstTime = frequence;

        this.executeThreadHbTime = TimeUtils.current_time_secs();
        this.taskHbTimeout = JStormUtils.parseInt(conf.get(Config.NIMBUS_TASK_TIMEOUT_SECS), 180);
        this.intervalCheck = new IntervalCheck();
        this.intervalCheck.setInterval(taskHbTimeout);
        this.intervalCheck.start();

        this.reportError = reportError;

        this.uptime = new UptimeComputer();
    }

    @Override
    public void updateObject() {
        this.object = Integer.valueOf(taskId);
    }

    @Override
    public void run() {
        try {
            updateObject();

            if (object == null) {
                LOG.info("Timer " + name + " 's object is null ");
                return;
            }

            if (intervalCheck.check()) {
                checkExecuteThreadHb();
            }

            if (componentId.equals(Common.TOPOLOGY_MASTER_COMPONENT_ID)) {
                Values values = new Values(uptime.uptime());
                TupleExt tuple = new TupleImplExt(sysTopologyCtx, values, taskId, Common.TOPOLOGY_MASTER_HB_STREAM_ID);
                queue.publish(tuple);
            } else {
                // Send task heartbeat to topology master
                sendHbMsg();
            }

            // Send message used to monitor execute thread 
            TimerEvent event = new TimerEvent(opCode, object);
            boolean ret = controlQueue.offer(event);
            if (ret)
                LOG.debug("Offer task HB event to controlQueue, taskId=" + taskId);
            else
                LOG.debug("Failed to offer task HB event to controlQueue, taskId=" + taskId);
        } catch (Exception e) {
            LOG.warn("Failed to publish timer event to " + name, e);
            return;
        }

        LOG.debug(" Trigger timer event to " + name);

    }

    public void setSpoutOutputCollector(SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void setBoltOutputCollector(OutputCollector boltOutputCollector) {
        this.boltOutputCollector = boltOutputCollector;
    }

    public void setExeThreadHbTime(long hbTime) {
        this.executeThreadHbTime = hbTime;
    }

    private void sendHbMsg() {
        List values = JStormUtils.mk_list(uptime.uptime());
        if (spoutOutputCollector != null) {
            spoutOutputCollector.emit(Common.TOPOLOGY_MASTER_HB_STREAM_ID, values);
        } else if (boltOutputCollector != null) {
            boltOutputCollector.emit(Common.TOPOLOGY_MASTER_HB_STREAM_ID, values);
        } else {
            LOG.warn("Failed to send hearbeat msg. OutputCollector has not been initialized!");
        }
    }

    private void checkExecuteThreadHb() {
        long currentTime = TimeUtils.current_time_secs();
        if (currentTime - executeThreadHbTime > taskHbTimeout) {
            String error = "No response from Task-" + taskId + ", last report time(sec) is " + executeThreadHbTime;
            reportError.report(error);
        }
    }
}