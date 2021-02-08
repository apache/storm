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

package org.apache.storm.topology;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicBoltExecutor implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(BasicBoltExecutor.class);

    private IBasicBolt bolt;
    private transient BasicOutputCollector collector;

    public BasicBoltExecutor(IBasicBolt bolt) {
        this.bolt = bolt;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        bolt.prepare(topoConf, context);
        this.collector = new BasicOutputCollector(collector);
    }

    @Override
    public void execute(Tuple input) {
        collector.setContext(input);
        try {
            bolt.execute(input, collector);
            collector.getOutputter().ack(input);
        } catch (FailedException e) {
            if (e instanceof ReportedFailedException) {
                collector.reportError(e);
            }
            collector.getOutputter().fail(input);
        }
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }
}
