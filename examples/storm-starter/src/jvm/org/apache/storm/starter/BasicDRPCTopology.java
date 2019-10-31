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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It implements a function that appends a
 * "!" to any string you send the DRPC function.
 *
 * @see <a href="http://storm.apache.org/documentation/Distributed-RPC.html">Distributed RPC</a>
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class BasicDRPCTopology {
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        String topoName = "DRPCExample";
        String function = "exclamation";
        if (args != null) {
            if (args.length > 0) {
                topoName = args[0];
            }
            if (args.length > 1) {
                function = args[1];
            }
        }

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        builder.addBolt(new ExclaimBolt(), 3);

        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createRemoteTopology());

        if (args != null && args.length > 2) {
            try (DRPCClient drpc = DRPCClient.getConfiguredClient(conf)) {
                for (int i = 2; i < args.length; i++) {
                    String word = args[i];
                    System.out.println("Result for \"" + word + "\": " + drpc.execute(function, word));
                }
            }
        }
    }

    public static class ExclaimBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String input = tuple.getString(1);
            collector.emit(new Values(tuple.getValue(0), input + "!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }
}
