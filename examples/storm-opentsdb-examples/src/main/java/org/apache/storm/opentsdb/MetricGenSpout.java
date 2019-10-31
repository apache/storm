/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.opentsdb;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.storm.opentsdb.bolt.TupleOpenTsdbDatapointMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

/**
 * Spout to generate tuples containing metric data.
 */
public class MetricGenSpout extends BaseRichSpout {

    public static final Fields DEFAULT_METRIC_FIELDS =
            new Fields(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER.getMetricField(),
                    TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER.getTimestampField(),
                    TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER.getValueField(),
                    TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER.getTagsField());

    private Map<String, Object> conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(DEFAULT_METRIC_FIELDS);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // ignore
        }
        // tuple values are mapped with
        // metric, timestamp, value, Map of tagK/tagV respectively.
        collector.emit(Lists.newArrayList("device.temp", System.currentTimeMillis(), new Random().nextLong(),
                Collections.singletonMap("loc.id", new Random().nextInt() % 64 + "")));
    }
}
