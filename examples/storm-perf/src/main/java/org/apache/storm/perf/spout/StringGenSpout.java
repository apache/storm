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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

/**
 * Spout pre-computes a list with 30k fixed length random strings. Emits sequentially from this list, over and over again.
 */

public class StringGenSpout extends BaseRichSpout {

    private static final String DEFAULT_FIELD_NAME = "str";
    private final int strCount = 30_000;
    ArrayList<String> records;
    private int strLen;
    private String fieldName = DEFAULT_FIELD_NAME;
    private SpoutOutputCollector collector = null;
    private int curr = 0;
    private int count = 0;

    public StringGenSpout(int strLen) {
        this.strLen = strLen;
    }

    private static ArrayList<String> genStringList(int strLen, int count) {
        ArrayList<String> result = new ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            result.add(RandomStringUtils.random(strLen));
        }
        return result;
    }

    public StringGenSpout withFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fieldName));
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.records = genStringList(strLen, strCount);

        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        List<Object> tuple;
        if (curr < strCount) {
            tuple = Collections.singletonList((Object) records.get(curr));
            ++curr;
            collector.emit(tuple, ++count);
        }
    }


    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }
}
