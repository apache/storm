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

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger.TimerEvent;
import com.alibaba.jstorm.utils.JStormUtils;

public class TaskHeartbeatTrigger extends TimerTrigger {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskHeartbeatTrigger.class);

    private int taskId;
    
    private BlockingQueue<Object> controlQueue;

    public TaskHeartbeatTrigger(Map conf, String name, DisruptorQueue queue,
            BlockingQueue<Object> controlQueue, int taskId) {
        this.name = name;
        this.queue = queue;
        this.controlQueue = controlQueue;
        this.opCode = TimerConstants.TASK_HEARTBEAT;

        this.taskId = taskId;

        frequence =
                JStormUtils.parseInt(
                        conf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 10);

        firstTime = frequence;
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

            TimerEvent event = new TimerEvent(opCode, object);
            
            controlQueue.offer(event);
            LOG.debug("Offer task HB event to controlQueue, taskId=" + taskId);
        } catch (Exception e) {
            LOG.warn("Failed to public timer event to " + name, e);
            return;
        }

        LOG.debug(" Trigger timer event to " + name);

    }
}