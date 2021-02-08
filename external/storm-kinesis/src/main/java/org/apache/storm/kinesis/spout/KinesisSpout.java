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

package org.apache.storm.kinesis.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

public class KinesisSpout extends BaseRichSpout {

    private final KinesisConfig kinesisConfig;
    private transient KinesisRecordsManager kinesisRecordsManager;
    private transient SpoutOutputCollector collector;

    public KinesisSpout(KinesisConfig kinesisConfig) {
        this.kinesisConfig = kinesisConfig;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(kinesisConfig.getRecordToTupleMapper().getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        kinesisRecordsManager = new KinesisRecordsManager(kinesisConfig);
        kinesisRecordsManager.initialize(context.getThisTaskIndex(), context.getComponentTasks(context.getThisComponentId()).size());
    }

    @Override
    public void close() {
        kinesisRecordsManager.close();
    }

    @Override
    public void activate() {
        kinesisRecordsManager.activate();
    }

    @Override
    public void deactivate() {
        kinesisRecordsManager.deactivate();
    }

    @Override
    public void ack(Object msgId) {
        kinesisRecordsManager.ack((KinesisMessageId) msgId);
    }

    @Override
    public void fail(Object msgId) {
        kinesisRecordsManager.fail((KinesisMessageId) msgId);
    }

    @Override
    public void nextTuple() {
        kinesisRecordsManager.next(collector);
    }
}


