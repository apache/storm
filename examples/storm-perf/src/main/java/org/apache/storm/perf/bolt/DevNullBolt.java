/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf.bolt;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.LoggerFactory;


public class DevNullBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DevNullBolt.class);
    private OutputCollector collector;
    private Long sleepNanos;
    private int count = 0;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sleepNanos = ObjectReader.getLong(topoConf.get("nullbolt.sleep.micros"), 0L) * 1_000;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        if (sleepNanos > 0) {
            LockSupport.parkNanos(sleepNanos);
        }
        ++count;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
