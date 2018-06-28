/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.topology.window;

import static org.apache.storm.st.topology.TestableTopology.DUMMY_FIELD;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeDataVerificationBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TimeDataVerificationBolt.class);
    private OutputCollector collector;
    private String componentId;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        componentId = context.getThisComponentId();
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();
        LOG.info("tuplesInWindow.size() = " + tuplesInWindow.size());
        LOG.info("newTuples.size() = " + newTuples.size());
        LOG.info("expiredTuples.size() = " + expiredTuples.size());
        List<TimeData> dataInWindow = tuplesInWindow.stream()
            .map(TimeData::fromTuple)
            .collect(Collectors.toList());
        final String jsonData = TimeData.toString(dataInWindow);
        LOG.info(StringDecorator.decorate(componentId, jsonData));
        collector.emit(new Values("dummyValue"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DUMMY_FIELD));
    }
}