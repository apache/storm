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

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic bolt for storing tuple to ES document.
 */
public class EsIndexBolt extends AbstractEsBolt {
    private final EsTupleMapper tupleMapper;
    private MODE executeMode = MODE.SYNC;
    private long batchSize = 1;
    private List<Tuple> tupleBatch;
    private int tickTupleInterval;

    /**
     * EsIndexBolt constructor
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    public EsIndexBolt(EsConfig esConfig, EsTupleMapper tupleMapper) {
        super(esConfig);
        this.tupleMapper = checkNotNull(tupleMapper, "tupleMapper cannot be null");
        this.tupleBatch = new LinkedList<>();
    }

    /**
     *
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     * @param executeMode A parameter specifying synchronous or asychnronous messages to Elastic
     * @param batchSize Batch size for sending messages to Elastic.  Set to 1 for no batching
     * @param tickTupleInterval A value > 0 will produce tick tuples
     */
    public EsIndexBolt(EsConfig esConfig, EsTupleMapper tupleMapper, MODE executeMode, long batchSize, int tickTupleInterval) {
        this(esConfig, tupleMapper);
        this.executeMode = checkNotNull(executeMode, "executeMode cannot be null");
        this.batchSize = batchSize;
        this.tickTupleInterval = tickTupleInterval;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = super.getComponentConfiguration();
        if (conf == null)
            conf = new Config();

        if (this.tickTupleInterval > 0) {
            LOG.info("Enabling tick tuple with interval [{}]", tickTupleInterval);
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTupleInterval);
        }

        return conf;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }

    /**
     * {@inheritDoc}
     * Tuple should have relevant fields (source, index, type, id) for tupleMapper to extract ES document.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            boolean forceWrite = false;
            if (isTickTuple(tuple))
            {
                LOG.debug("TICK received! current batch status [" + tupleBatch.size() + "/" + this.batchSize + "]");
                forceWrite = true;
            } else {
                tupleBatch.add(tuple);
                if (tupleBatch.size() >= batchSize) {
                    forceWrite = true;
                }
            }

            if (forceWrite && tupleBatch.size() > 0) {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (final Tuple t : tupleBatch) {
                    String source = tupleMapper.getSource(t);
                    String index = tupleMapper.getIndex(t);
                    String type = tupleMapper.getType(t);
                    String id = tupleMapper.getId(t);

                    bulkRequest.add(client.prepareIndex(index, type, id).setSource(source));
                }
                if (executeMode == MODE.SYNC) {
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        LOG.error("Bulk request errors [" + bulkResponse.buildFailureMessage() + "]");
                        throw new RuntimeException(bulkResponse.buildFailureMessage());
                    }
                } else if (executeMode == MODE.ASYNC) {
                    bulkRequest.execute();
                }
                for (final Tuple t: tupleBatch)
                    collector.ack(t);
                tupleBatch.clear();
            }
        } catch (Exception e) {
            collector.reportError(e);
            for (final Tuple t: tupleBatch)
                collector.fail(t);
            tupleBatch.clear();
            System.exit(1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public enum MODE {
        SYNC, ASYNC
    }

    //FIXME -- need our own version of this until we use a later version of Storm
    private static boolean isTickTuple(Tuple tuple) {
        if (tuple.getSourceComponent() == null || tuple.getSourceStreamId() == null)
            return false;

        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }
}
