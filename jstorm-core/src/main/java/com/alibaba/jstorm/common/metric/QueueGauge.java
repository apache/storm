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
package com.alibaba.jstorm.common.metric;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.health.HealthCheck;

public class QueueGauge extends HealthCheck implements Gauge<Double> {
    private static final Logger LOG = LoggerFactory.getLogger(QueueGauge.class);

    public static final String QUEUE_IS_FULL = " is full";

    DisruptorQueue queue;
    String name;
    Result healthy;

    public QueueGauge(DisruptorQueue queue, String... names) {
        this.queue = queue;
        this.name = Joiner.on("-").join(names);
        this.healthy = Result.healthy();
    }

    @Override
    public Double getValue() {
        Double ret = (double) queue.pctFull();

        return ret;
    }

    @Override
    protected Result check() throws Exception {
        // TODO Auto-generated method stub
        Double ret = (double) queue.pctFull();
        if (ret > 0.9) {
            return Result.unhealthy(name + QUEUE_IS_FULL);
        } else {
            return healthy;
        }
    }

}
