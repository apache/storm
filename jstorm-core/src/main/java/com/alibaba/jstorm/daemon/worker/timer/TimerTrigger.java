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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.InsufficientCapacityException;

public class TimerTrigger implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(TimerTrigger.class);

    private static ScheduledExecutorService threadPool;

    public static void setScheduledExecutorService(
            ScheduledExecutorService scheduledExecutorService) {
        threadPool = scheduledExecutorService;
    }

    protected String name;
    protected int opCode;
    protected int firstTime;
    protected int frequence;
    protected DisruptorQueue queue;
    protected Object object;
    protected boolean block = true;

    public void register() {
        register(TimeUnit.SECONDS);
    }

    public void register(TimeUnit timeUnit) {
        threadPool.scheduleAtFixedRate(this, firstTime, frequence,
                timeUnit);
        LOG.info("Successfully register timer " + this);
    }

    public void updateObject() {

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
            queue.publish(event, block);
        } catch (InsufficientCapacityException e) {
            LOG.warn("Failed to public timer event to " + name);
            return;
        } catch (Exception e) {
            LOG.warn("Failed to public timer event to " + name, e);
            return;
        }

        LOG.debug(" Trigger timer event to " + name);

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getOpCode() {
        return opCode;
    }

    public void setOpCode(int opCode) {
        this.opCode = opCode;
    }

    public int getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(int firstTime) {
        this.firstTime = firstTime;
    }

    public int getFrequence() {
        return frequence;
    }

    public void setFrequence(int frequence) {
        this.frequence = frequence;
    }

    public DisruptorQueue getQueue() {
        return queue;
    }

    public void setQueue(DisruptorQueue queue) {
        this.queue = queue;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public boolean isBlock() {
        return block;
    }

    public void setBlock(boolean block) {
        this.block = block;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public class TimerEvent {
        private int opCode;
        private Object msg;

        public TimerEvent(int opCode, Object msg) {
            this.opCode = opCode;
            this.msg = msg;
        }

        public int getOpCode() {
            return opCode;
        }

        public Object getMsg() {
            return msg;
        }
    }
}
