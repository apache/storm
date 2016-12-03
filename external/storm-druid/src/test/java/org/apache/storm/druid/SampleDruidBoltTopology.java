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
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.druid.bolt.DruidBeamBolt;
import org.apache.storm.druid.bolt.DruidBeamFactory;
import org.apache.storm.druid.bolt.DruidConfig;
import org.apache.storm.druid.bolt.ITupleDruidEventMapper;
import org.apache.storm.druid.bolt.TupleDruidEventMapper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Sample application to use Druid bolt.
 *
 * To test this we need to deploy Druid application. Refer Druid quickstart to run druid.
 * http://druid.io/docs/latest/tutorials/quickstart.html
 */
public class SampleDruidBoltTopology {

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
          throw new IllegalArgumentException("There should be at least one argument. Run as `SampleDruidBoltTopology <zk-url>`");
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("event-gen", new SimpleSpout(), 5);
        DruidBeamFactory druidBeamFactory = new SampleDruidBeamFactoryImpl(new HashMap<String, Object>());
        DruidConfig.Builder builder = DruidConfig.newBuilder().discardStreamId(DruidConfig.DEFAULT_DISCARD_STREAM_ID);
        ITupleDruidEventMapper<Map<String, Object>> eventMapper = new TupleDruidEventMapper<>(TupleDruidEventMapper.DEFAULT_FIELD_NAME);
        DruidBeamBolt<Map<String, Object>> druidBolt = new DruidBeamBolt<Map<String, Object>>(druidBeamFactory, eventMapper, builder);
        topologyBuilder.setBolt("druid-bolt", druidBolt).shuffleGrouping("event-gen");
        topologyBuilder.setBolt("printer-bolt", new PrinterBolt()).shuffleGrouping("druid-bolt" , DruidConfig.DEFAULT_DISCARD_STREAM_ID);

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("druid.tranquility.zk.connect", args[0]);

        if (args.length > 1) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, topologyBuilder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            try (LocalCluster cluster = new LocalCluster();
                 LocalTopology topo = cluster.submitTopology("druid-test", conf, topologyBuilder.createTopology());) {
                Thread.sleep(30000);
            }
            System.exit(0);
        }
    }

    private static class PrinterBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }

    }

}
