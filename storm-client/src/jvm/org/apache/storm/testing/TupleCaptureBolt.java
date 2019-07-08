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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class TupleCaptureBolt implements IRichBolt {

    /*
     * Even though normally bolts do not need to care about thread safety, this particular bolt is different.
     * It maintains a static field that is prepopulated before the topology starts, is written into by the topology,
     * and is then read from after the topology is completed - all of this by potentially different threads.
     */

    private static final transient Map<String, Map<String, List<FixedTuple>>> emitted_tuples = new ConcurrentHashMap<>();

    private final String name;
    private OutputCollector collector;

    public TupleCaptureBolt() {
        name = UUID.randomUUID().toString();
        emitted_tuples.put(name, new ConcurrentHashMap<String, List<FixedTuple>>());
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String component = input.getSourceComponent();
        emitted_tuples.get(name)
            .compute(component, (String key, List<FixedTuple> tuples) -> {
                if (tuples == null) {
                    tuples = new ArrayList<>();
                }
                tuples.add(new FixedTuple(input.getSourceStreamId(), input.getValues()));
                return tuples;
            });
        collector.ack(input);
    }

    public Map<String, List<FixedTuple>> getResults() {
        return emitted_tuples.get(name);
    }

    @Override
    public void cleanup() {
    }

    public Map<String, List<FixedTuple>> getAndRemoveResults() {
        return emitted_tuples.remove(name);
    }

    public Map<String, List<FixedTuple>> getAndClearResults() {
        Map<String, List<FixedTuple>> results = emitted_tuples.get(name);
        Map<String, List<FixedTuple>> ret = new HashMap<>(results);
        results.clear();
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
