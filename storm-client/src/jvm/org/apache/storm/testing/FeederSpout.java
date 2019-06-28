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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.InprocMessaging;


public class FeederSpout extends BaseRichSpout {
    private int id;
    private Fields outFields;
    private SpoutOutputCollector collector;
    private AckFailDelegate ackFailDelegate;

    public FeederSpout(List<String> outFields) {
        this(new Fields(outFields));
    }

    public FeederSpout(Fields outFields) {
        id = InprocMessaging.acquireNewPort();
        this.outFields = outFields;
    }

    public void setAckFailDelegate(AckFailDelegate d) {
        ackFailDelegate = d;
    }

    public void feed(List<Object> tuple) {
        feed(tuple, UUID.randomUUID().toString());
    }

    public void feed(List<Object> tuple, Object msgId) {
        InprocMessaging.sendMessage(id, new Values(tuple, msgId));
    }

    public void feedNoWait(List<Object> tuple, Object msgId) {
        InprocMessaging.sendMessageNoWait(id, new Values(tuple, msgId));
    }

    public void waitForReader() {
        InprocMessaging.waitForReader(id);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void nextTuple() {
        List<Object> toEmit = (List<Object>) InprocMessaging.pollMessage(id);
        if (toEmit != null) {
            List<Object> tuple = (List<Object>) toEmit.get(0);
            Object msgId = toEmit.get(1);

            collector.emit(tuple, msgId);
        }
    }

    @Override
    public void ack(Object msgId) {
        if (ackFailDelegate != null) {
            ackFailDelegate.ack(msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        if (ackFailDelegate != null) {
            ackFailDelegate.fail(msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outFields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
