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
package org.apache.storm.druid;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * BatchSpout implementation for event batch  generation.
 */
public class SimpleBatchSpout implements IBatchSpout {

    private int batchSize;
    private final Map<Long, List<List<Object>>> batches = new HashMap<>();

    public SimpleBatchSpout(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> values;
        if(batches.containsKey(batchId)) {
            values = batches.get(batchId);
        } else {
            values = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                List<Object> value = new ArrayList<>();
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", new DateTime().toString());
                event.put("publisher", "foo.com");
                event.put("advertiser", "google.com");
                event.put("click", i);
                value.add(event);
                values.add(value);
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
        return SimpleSpout.DEFAULT_FIELDS;
    }
}
