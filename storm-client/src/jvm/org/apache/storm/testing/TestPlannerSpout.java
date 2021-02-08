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
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


public class TestPlannerSpout extends BaseRichSpout {
    boolean isDistributed;
    Fields outFields;

    public TestPlannerSpout(Fields outFields, boolean isDistributed) {
        this.isDistributed = isDistributed;
        this.outFields = outFields;
    }

    public TestPlannerSpout(boolean isDistributed) {
        this(new Fields("field1", "field2"), isDistributed);
    }

    public TestPlannerSpout(Fields outFields) {
        this(outFields, true);
    }

    public Fields getOutputFields() {
        return outFields;
    }


    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

    }

    @Override
    public void close() {

    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        if (!isDistributed) {
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        return ret;
    }
}
