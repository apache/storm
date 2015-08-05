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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.utils.TimeUtils;

public class TickTupleTrigger extends TimerTrigger {
    private static final Logger LOG = LoggerFactory
            .getLogger(TickTupleTrigger.class);

    TopologyContext topologyContext;

    public TickTupleTrigger(TopologyContext topologyContext, int frequence,
            String name, DisruptorQueue queue) {
        this.name = name;
        this.queue = queue;
        this.opCode = TimerConstants.TICK_TUPLE;

        if (frequence <= 0) {
            LOG.warn(" The frequence of " + name + " is invalid");
            frequence = 1;
        }
        this.firstTime = frequence;
        this.frequence = frequence;
        this.topologyContext = topologyContext;

    }

    @Override
    public void updateObject() {
        this.object =
                new TupleImplExt(topologyContext, new Values(
                        TimeUtils.current_time_secs()),
                        (int) Constants.SYSTEM_TASK_ID,
                        Constants.SYSTEM_TICK_STREAM_ID);
    }

}
