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

import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.RollingCountAggBolt;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This topology does a continuous computation of the top N words that the
 * topology has seen in terms of cardinality. The top N computation is done in a
 * completely scalable way, and a similar approach could be used to compute
 * things like trending topics or trending images on Twitter. It takes an
 * approach that assumes that some works will be much more common then other
 * words, and uses partialKeyGrouping to better balance the skewed load.
 */
public class SkewedRollingTopWords extends ConfigurableTopology {

    private static final Logger LOG = LoggerFactory.getLogger(SkewedRollingTopWords.class);
    private static final int TOP_N = 5;

    private SkewedRollingTopWords() {
    }

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new SkewedRollingTopWords(), args);
    }

    /**
     * Submits (runs) the topology.
     *
     * <p>Usage: "SkewedRollingTopWords [topology-name] [-local]"
     *
     * <p>By default, the topology is run locally under the name
     * "slidingWindowCounts".
     *
     * <p>Examples:
     *
     * <p>```
     * # Runs in remote/cluster mode, with topology name "production-topology"
     * $ storm jar storm-starter-jar-with-dependencies.jar org.apache.storm.starter.SkewedRollingTopWords production-topology ```
     *
     * @param args
     *          First positional argument (optional) is topology name, second
     *          positional argument (optional) defines whether to run the topology
     *          locally ("-local") or remotely, i.e. on a real cluster
     */
    @Override
    protected int run(String[] args) {
        String topologyName = "slidingWindowCounts";
        if (args.length >= 1) {
            topologyName = args[0];
        }
        TopologyBuilder builder = new TopologyBuilder();
        String spoutId = "wordGenerator";
        String counterId = "counter";
        String aggId = "aggregator";
        builder.setSpout(spoutId, new TestWordSpout(), 5);
        builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).partialKeyGrouping(spoutId, new Fields("word"));
        builder.setBolt(aggId, new RollingCountAggBolt(), 4).fieldsGrouping(counterId, new Fields("obj"));
        String intermediateRankerId = "intermediateRanker";
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(aggId, new Fields("obj"));
        String totalRankerId = "finalRanker";
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
        LOG.info("Topology name: " + topologyName);

        return submit(topologyName, conf, builder);
    }
}
