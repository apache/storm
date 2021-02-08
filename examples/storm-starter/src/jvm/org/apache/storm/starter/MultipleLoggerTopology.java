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

package org.apache.storm.starter;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a basic example of a Storm topology.
 */
public class MultipleLoggerTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationLoggingBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationLoggingBolt(), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(true);
        String topoName = MultipleLoggerTopology.class.getName();
        if (args != null && args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(2);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }

    public static class ExclamationLoggingBolt extends BaseRichBolt {
        OutputCollector collector;
        Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        // ensure the loggers are configured in the worker.xml before
        // trying to use them here
        Logger logger = LoggerFactory.getLogger("com.myapp");
        Logger subLogger = LoggerFactory.getLogger("com.myapp.sub");

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            rootLogger.debug("root: This is a DEBUG message");
            rootLogger.info("root: This is an INFO message");
            rootLogger.warn("root: This is a WARN message");
            rootLogger.error("root: This is an ERROR message");

            logger.debug("myapp: This is a DEBUG message");
            logger.info("myapp: This is an INFO message");
            logger.warn("myapp: This is a WARN message");
            logger.error("myapp: This is an ERROR message");

            subLogger.debug("myapp.sub: This is a DEBUG message");
            subLogger.info("myapp.sub: This is an INFO message");
            subLogger.warn("myapp.sub: This is a WARN message");
            subLogger.error("myapp.sub: This is an ERROR message");

            collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
}
