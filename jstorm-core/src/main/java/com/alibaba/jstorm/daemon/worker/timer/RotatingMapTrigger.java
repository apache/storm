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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.utils.JStormUtils;

public class RotatingMapTrigger extends TimerTrigger {
    private static final Logger LOG = LoggerFactory
            .getLogger(RotatingMapTrigger.class);

    public RotatingMapTrigger(Map conf, String name, DisruptorQueue queue) {
        this.name = name;
        this.queue = queue;
        this.opCode = TimerConstants.ROTATING_MAP;

        int msgTimeOut =
                JStormUtils.parseInt(
                        conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
        frequence = (msgTimeOut) / (Acker.TIMEOUT_BUCKET_NUM - 1);
        if (frequence <= 0) {
            frequence = 1;
        }

        firstTime =
                JStormUtils.parseInt(
                        conf.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS),
                        120);

        firstTime += frequence;
    }

    @Override
    public void updateObject() {
        this.object = new Tick(name);
    }

    public static final String ROTATINGMAP_STREAMID = "__rotating_tick";

    // In fact, RotatingMapTrigger can use TickTuple,
    // which set the stream ID is ROTATINGMAP_STREAMID
    // But in order to improve performance, JStorm use RotatingMapTrigger.Tick

    public static class Tick {
        private final long time;
        private final String name;

        public Tick(String name) {
            this.name = name;
            time = System.currentTimeMillis();
        }

        public long getTime() {
            return time;
        }

        public String getName() {
            return name;
        }

    }

}
