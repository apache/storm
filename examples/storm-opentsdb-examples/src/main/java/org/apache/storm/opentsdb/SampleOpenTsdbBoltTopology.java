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

import java.util.Collections;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.opentsdb.bolt.OpenTsdbBolt;
import org.apache.storm.opentsdb.bolt.TupleOpenTsdbDatapointMapper;
import org.apache.storm.opentsdb.client.OpenTsdbClient;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Sample application to use OpenTSDB bolt.
 */
public class SampleOpenTsdbBoltTopology {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("There should be at least one argument. Run as `SampleOpenTsdbBoltTopology <tsdb-url>`");
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("metric-gen", new MetricGenSpout(), 5);

        String openTsdbUrl = args[0];
        OpenTsdbClient.Builder builder =  OpenTsdbClient.newBuilder(openTsdbUrl).sync(30_000).returnDetails();
        final OpenTsdbBolt openTsdbBolt = new OpenTsdbBolt(builder, Collections.singletonList(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER));
        openTsdbBolt.withBatchSize(10).withFlushInterval(2).failTupleForFailedMetrics();
        topologyBuilder.setBolt("opentsdb", openTsdbBolt).shuffleGrouping("metric-gen");

        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "word-count";
        if (args.length > 1) {
            topoName = args[1];
        }
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topologyBuilder.createTopology());
    }
}
