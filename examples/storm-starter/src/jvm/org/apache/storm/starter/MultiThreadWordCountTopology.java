/*
  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
  2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.
 */

package org.apache.storm.starter;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.starter.bolt.WordCountBolt;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Some topologies might spawn some threads within bolts to do some work and emit tuples from those threads.
 * This is a simple wordcount topology example that mimics those use cases and might help us catch possible race conditions.
 */
public class MultiThreadWordCountTopology extends ConfigurableTopology {
    public static void main(String[] args) {
        ConfigurableTopology.start(new MultiThreadWordCountTopology(), args);
    }

    @Override
    protected int run(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new MultiThreadedSplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

        //this makes sure there is only one executor per worker, easier to debug
        //problems involving serialization/deserialization will only happen in inter-worker data transfer
        conf.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, true);
        //this involves metricsTick
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        conf.setTopologyWorkerMaxHeapSize(128);

        String topologyName = "multithreaded-word-count";

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }

    public static class MultiThreadedSplitSentence implements IRichBolt {

        private OutputCollector collector;
        private ExecutorService executor;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            executor = Executors.newFixedThreadPool(6);
            //This makes sure metricsTick to be called every 1 second
            //it makes the race condition between metricsTick and outputCollector easier to happen if any
            context.registerMetric("dummy-counter", () -> 0, 1);
        }

        @Override
        public void execute(Tuple input) {
            String str = input.getString(0);
            String[] splits = str.split("\\s+");
            for (String s : splits) {
                //spawn other threads to do the work and emit
                Runnable runnableTask = () -> {
                    collector.emit(new Values(s));
                };
                executor.submit(runnableTask);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void cleanup() {
        }
    }
}