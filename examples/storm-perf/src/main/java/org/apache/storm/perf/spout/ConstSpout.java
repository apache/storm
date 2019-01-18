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

package org.apache.storm.perf.spout;

import java.util.ArrayList;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ObjectReader;

public class ConstSpout extends BaseRichSpout {

    private static final String DEFAUT_FIELD_NAME = "str";
    private String value;
    private String fieldName = DEFAUT_FIELD_NAME;
    private SpoutOutputCollector collector = null;
    private int count = 0;
    private Long sleep = 0L;
    private int ackCount = 0;

    public ConstSpout(String value) {
        this.value = value;
    }

    public ConstSpout withOutputFields(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fieldName));
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.sleep = ObjectReader.getLong(conf.get("spout.sleep"), 0L);
    }

    @Override
    public void nextTuple() {
        ArrayList<Object> tuple = new ArrayList<Object>(1);
        tuple.add(value);
        collector.emit(tuple, count++);
        try {
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        } catch (InterruptedException e) {
            return;
        }
    }

    @Override
    public void ack(Object msgId) {
        ++ackCount;
        super.ack(msgId);
    }

}
