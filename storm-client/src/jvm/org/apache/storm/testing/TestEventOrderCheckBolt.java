/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.testing;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEventOrderCheckBolt extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestEventOrderCheckBolt.class);
    OutputCollector collector;
    Map<Integer, Long> recentEventId = new HashMap<Integer, Long>();
    private int count;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        count = 0;
    }

    @Override
    public void execute(Tuple input) {
        Integer sourceId = input.getInteger(0);
        Long eventId = input.getLong(1);
        Long recentEvent = recentEventId.get(sourceId);

        if (null != recentEvent && eventId <= recentEvent) {
            String error = "Error: event id is not in strict order! event source Id: "
                           + sourceId + ", last event Id: " + recentEvent + ", current event Id: " + eventId;

            collector.emit(input, new Values(error));
        }
        recentEventId.put(sourceId, eventId);

        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    public Fields getOutputFields() {
        return new Fields("error");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("error"));
    }
}
