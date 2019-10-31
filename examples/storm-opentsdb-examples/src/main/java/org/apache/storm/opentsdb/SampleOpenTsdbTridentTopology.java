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
import org.apache.storm.opentsdb.bolt.TupleOpenTsdbDatapointMapper;
import org.apache.storm.opentsdb.client.OpenTsdbClient;
import org.apache.storm.opentsdb.trident.OpenTsdbStateFactory;
import org.apache.storm.opentsdb.trident.OpenTsdbStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample trident topology to store time series metrics in to OpenTsdb.
 */
public class SampleOpenTsdbTridentTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SampleOpenTsdbTridentTopology.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("There should be at least one argument. Run as `SampleOpenTsdbTridentTopology <tsdb-url>`");
        }

        String tsdbUrl = args[0];


        final OpenTsdbClient.Builder openTsdbClientBuilder = OpenTsdbClient.newBuilder(tsdbUrl);
        final OpenTsdbStateFactory openTsdbStateFactory =
                new OpenTsdbStateFactory(openTsdbClientBuilder,
                        Collections.singletonList(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER));

        TridentTopology tridentTopology = new TridentTopology();
        final Stream stream = tridentTopology.newStream("metric-tsdb-stream", new MetricGenBatchSpout(10));

        stream.peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                LOG.info("########### Received tuple: [{}]", input);
            }
        }).partitionPersist(openTsdbStateFactory, MetricGenSpout.DEFAULT_METRIC_FIELDS, new OpenTsdbStateUpdater());


        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "word-count";
        if (args.length > 1) {
            topoName = args[1];
        }
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, tridentTopology.build());
    }
}
