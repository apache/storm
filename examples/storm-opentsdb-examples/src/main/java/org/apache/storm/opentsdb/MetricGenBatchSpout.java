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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

/**
 * BatchSpout implementation for metrics generation.
 */
public class MetricGenBatchSpout implements IBatchSpout {

    private int batchSize;
    private final Map<Long, List<List<Object>>> batches = new HashMap<>();

    public MetricGenBatchSpout(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context) {

    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> values;
        if (batches.containsKey(batchId)) {
            values = batches.get(batchId);
        } else {
            values = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                // tuple values are mapped with
                // metric, timestamp, value, Map of tagK/tagV respectively.
                values.add(Lists.newArrayList(Lists.newArrayList("device.temp", System.currentTimeMillis(), new Random().nextLong(),
                        Collections.singletonMap("loc.id", new Random().nextInt() % 64 + ""))));
            }
            batches.put(batchId, values);
        }
        for (List<Object> value : values) {
            collector.emit(value);
        }

    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return MetricGenSpout.DEFAULT_METRIC_FIELDS;
    }
}
