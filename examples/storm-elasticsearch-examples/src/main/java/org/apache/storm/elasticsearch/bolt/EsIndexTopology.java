/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.elasticsearch.bolt;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsConstants;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Demonstrates an ElasticSearch Storm topology.
 */
public final class EsIndexTopology {

    /**
     * The id of the used spout.
     */
    private static final String SPOUT_ID = "spout";
    /**
     * The id of the used bolt.
     */
    private static final String BOLT_ID = "bolt";
    /**
     * The name of the used topology.
     */
    private static final String TOPOLOGY_NAME = "elasticsearch-test-topology1";
    /**
     * The number of pending tuples triggering logging.
     */
    private static final int PENDING_COUNT_MAX = 1000;

    /**
     * The example's main method.
     * @param args the command line arguments
     * @throws AlreadyAliveException if the topology is already started
     * @throws InvalidTopologyException if the topology is invalid
     * @throws AuthorizationException if the topology authorization fails
     */
    public static void main(final String[] args) throws AlreadyAliveException,
            InvalidTopologyException,
            AuthorizationException {
        Config config = new Config();
        config.setNumWorkers(1);
        TopologyBuilder builder = new TopologyBuilder();
        UserDataSpout spout = new UserDataSpout();
        builder.setSpout(SPOUT_ID, spout, 1);
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        EsConfig esConfig = new EsConfig("http://localhost:9300");
        builder.setBolt(BOLT_ID, new EsIndexBolt(esConfig, tupleMapper), 1)
                .shuffleGrouping(SPOUT_ID);

        EsTestUtil.startEsNode();
        EsTestUtil.waitForSeconds(EsConstants.WAIT_DEFAULT_SECS);
        StormSubmitter.submitTopology(TOPOLOGY_NAME,
                config,
                builder.createTopology());
    }

    /**
     * The user data spout.
     */
    public static class UserDataSpout extends BaseRichSpout {
        private static final long serialVersionUID = 1L;
        /**
         * The pending values.
         */
        private ConcurrentHashMap<UUID, Values> pending;
        /**
         * The collector passed in
         * {@link #open(java.util.Map, org.apache.storm.task.TopologyContext,
         * org.apache.storm.spout.SpoutOutputCollector) }.
         */
        private SpoutOutputCollector collector;
        private String[] sources = {
            "{\"user\":\"user1\"}",
            "{\"user\":\"user2\"}",
            "{\"user\":\"user3\"}",
            "{\"user\":\"user4\"}"
        };
        private int index = 0;
        private int count = 0;
        private long total = 0L;
        private String indexName = "index1";
        private String typeName = "type1";

        /**
         * Declares {@code source}, {@code index}, {@code type} and {@code id}.
         * @param declarer the declarer to pass to
         */
        @Override
        public void declareOutputFields(final OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("source", "index", "type", "id"));
        }

        /**
         * Acquires {@code collector} and initializes {@code pending}.
         * @param config unused
         * @param context unused
         * @param collectorArg the collector to acquire
         */
        @Override
        public void open(final Map<String, Object> config,
                final TopologyContext context,
                final SpoutOutputCollector collectorArg) {
            this.collector = collectorArg;
            this.pending = new ConcurrentHashMap<>();
        }

        /**
         * Makes the spout emit the next tuple, if any.
         */
        @Override
        public void nextTuple() {
            String source = sources[index];
            UUID msgId = UUID.randomUUID();
            Values values = new Values(source, indexName, typeName, msgId);
            this.pending.put(msgId, values);
            this.collector.emit(values, msgId);
            index++;
            if (index >= sources.length) {
                index = 0;
            }
            count++;
            total++;
            if (count > PENDING_COUNT_MAX) {
                count = 0;
                System.out.println("Pending count: " + this.pending.size()
                        + ", total: " + this.total);
            }
            Thread.yield();
        }

        /**
         * Acknowledges the message with id {@code msgId}.
         * @param msgId the message id
         */
        @Override
        public void ack(final Object msgId) {
            this.pending.remove(msgId);
        }

        /**
         * Marks the message with id {@code msgId} as failed.
         * @param msgId the message id
         */
        @Override
        public void fail(final Object msgId) {
            System.out.println("**** RESENDING FAILED TUPLE");
            this.collector.emit(this.pending.get(msgId), msgId);
        }
    }

    /**
     * Utility constructor to prevent initialization.
     */
    private EsIndexTopology() {
    }
}
