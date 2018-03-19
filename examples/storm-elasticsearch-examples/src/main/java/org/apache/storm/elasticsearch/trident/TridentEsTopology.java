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

package org.apache.storm.elasticsearch.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsConstants;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * A Trident topology example.
 */
public final class TridentEsTopology {
    private static final int BATCH_SIZE_DEFAULT = 100;
    private static final String TOPOLOGY_NAME = "elasticsearch-test-topology2";

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
        int batchSize = BATCH_SIZE_DEFAULT;
        FixedBatchSpout spout = new FixedBatchSpout(batchSize);
        spout.cycle = true;

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout", spout);
        EsConfig esConfig = new EsConfig("http://localhost:9300");
        Fields esFields = new Fields("index", "type", "source");
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        StateFactory factory = new EsStateFactory(esConfig, tupleMapper);
        TridentState state = stream.partitionPersist(factory,
                esFields,
                new EsUpdater(),
                new Fields());

        EsTestUtil.startEsNode();
        EsTestUtil.waitForSeconds(EsConstants.WAIT_DEFAULT_SECS);

        StormSubmitter.submitTopology(TOPOLOGY_NAME,
                new Config(),
                topology.build());
    }

    /**
     * A fixed batch spout.
     */
    public static class FixedBatchSpout implements IBatchSpout {
        private static final long serialVersionUID = 1L;
        private int maxBatchSize;
        /**
         * The passed batches.
         */
        private HashMap<Long, List<List<Object>>> batches = new HashMap<>();
        /**
         * The output values.
         */
        private Values[] outputs = {
            new Values("{\"user\":\"user1\"}",
                    "index1",
                    "type1",
                    UUID.randomUUID().toString()),
            new Values("{\"user\":\"user2\"}",
                    "index1",
                    "type2",
                    UUID.randomUUID().toString()),
            new Values("{\"user\":\"user3\"}",
                    "index2",
                    "type1",
                    UUID.randomUUID().toString()),
            new Values("{\"user\":\"user4\"}",
                    "index2",
                    "type2",
                    UUID.randomUUID().toString())
        };
        /**
         * The current index.
         */
        private int index = 0;
        /**
         * A flag indicating whether cycling ought to be performed.
         */
        private boolean cycle = false;

        /**
         * Creates a new fixed batch spout.
         * @param maxBatchSizeArg the maximum batch size to set
         */
        public FixedBatchSpout(final int maxBatchSizeArg) {
            this.maxBatchSize = maxBatchSizeArg;
        }

        /**
         * Gets the output fields.
         * @return the output fields.
         */
        @Override
        public Fields getOutputFields() {
            return new Fields("source", "index", "type", "id");
        }

        /**
         * Opens the spout.
         * @param conf the configuration to use for opening
         * @param context the context to use for opening
         */
        @Override
        public void open(final Map<String, Object> conf,
                final TopologyContext context) {
            index = 0;
        }

        /**
         * Emits a batch.
         * @param batchId the batch id to use
         * @param collector the collector to emit to
         */
        @Override
        public void emitBatch(final long batchId,
                final TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if (batch == null) {
                batch = new ArrayList<List<Object>>();
                if (index >= outputs.length && cycle) {
                    index = 0;
                }
                for (int i = 0; i < maxBatchSize; index++, i++) {
                    if (index == outputs.length) {
                        index = 0;
                    }
                    batch.add(outputs[index]);
                }
                this.batches.put(batchId, batch);
            }
            for (List<Object> list : batch) {
                collector.emit(list);
            }
        }

        /**
         * Acknowledges the message with id {@code msgId}.
         * @param batchId the message id
         */
        @Override
        public void ack(final long batchId) {
            this.batches.remove(batchId);
        }

        /**
         * Closes the spout.
         */
        @Override
        public void close() {
        }

        /**
         * Get the component configuration.
         * @return the component configuration
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }
    }

    /**
     * Utility constructor to prevent initialization.
     */
    private TridentEsTopology() {
    }
}
