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

package org.apache.storm.opentsdb.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.opentsdb.OpenTsdbMetricDatapoint;
import org.apache.storm.opentsdb.client.ClientResponse;
import org.apache.storm.opentsdb.client.OpenTsdbClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt implementation for storing timeseries datapoints to OpenTSDB.
 * <p/>
 * List of {@link OpenTsdbMetricDatapoint} is generated for each tuple with the configured {@code tupleOpenTsdbDatapointMappers}.
 * All these datapoints are batched till the given {@code batchSize} or {@code flushIntervalInSeconds} is reached.
 * <p/>
 *
 * <p>Example topology:
 * <blockquote><pre>
 * OpenTsdbClient.Builder builder =  OpenTsdbClient.newBuilder(openTsdbUrl).sync(30_000).returnDetails();
 * final OpenTsdbBolt openTsdbBolt = new OpenTsdbBolt(builder, Collections.singletonList(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER));
 * openTsdbBolt.withBatchSize(10).withFlushInterval(2).failTupleForFailedMetrics();
 * topologyBuilder.setBolt("opentsdb", openTsdbBolt).shuffleGrouping("metric-gen");
 * </pre></blockquote>
 *
 */
public class OpenTsdbBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTsdbBolt.class);

    private final OpenTsdbClient.Builder openTsdbClientBuilder;
    private final List<? extends ITupleOpenTsdbDatapointMapper> tupleOpenTsdbDatapointMappers;
    private int batchSize;
    private int flushIntervalInSeconds;
    private boolean failTupleForFailedMetrics;

    private BatchHelper batchHelper;
    private OpenTsdbClient openTsdbClient;
    private Map<OpenTsdbMetricDatapoint, Tuple> metricPointsWithTuple = new HashMap<>();
    private OutputCollector collector;

    public OpenTsdbBolt(OpenTsdbClient.Builder openTsdbClientBuilder,
            ITupleOpenTsdbDatapointMapper tupleOpenTsdbDatapointMapper) {
        this.openTsdbClientBuilder = openTsdbClientBuilder;
        this.tupleOpenTsdbDatapointMappers = Collections.singletonList(tupleOpenTsdbDatapointMapper);
    }

    public OpenTsdbBolt(OpenTsdbClient.Builder openTsdbClientBuilder,
            List<? extends ITupleOpenTsdbDatapointMapper> tupleOpenTsdbDatapointMappers) {
        this.openTsdbClientBuilder = openTsdbClientBuilder;
        this.tupleOpenTsdbDatapointMappers = tupleOpenTsdbDatapointMappers;
    }

    public OpenTsdbBolt withFlushInterval(int flushIntervalInSeconds) {
        this.flushIntervalInSeconds = flushIntervalInSeconds;
        return this;
    }

    public OpenTsdbBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * When it is invoked, this bolt acks only the tuples which have successful metrics stored into OpenTSDB and fails
     * the respective tuples of the failed metrics.
     *
     * @return same instance by setting {@code failTupleForFailedMetrics} to true
     */
    public OpenTsdbBolt failTupleForFailedMetrics() {
        this.failTupleForFailedMetrics = true;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        batchHelper = new BatchHelper(batchSize, collector);
        openTsdbClient = openTsdbClientBuilder.build();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                final List<OpenTsdbMetricDatapoint> metricDataPoints = getMetricPoints(tuple);
                for (OpenTsdbMetricDatapoint metricDataPoint : metricDataPoints) {
                    metricPointsWithTuple.put(metricDataPoint, tuple);
                }
                batchHelper.addBatch(tuple);
            }

            if (batchHelper.shouldFlush()) {
                LOG.debug("Sending metrics of size [{}]", metricPointsWithTuple.size());

                ClientResponse.Details clientResponse = openTsdbClient.writeMetricPoints(metricPointsWithTuple.keySet());

                if (failTupleForFailedMetrics && clientResponse != null && clientResponse.getFailed() > 0) {
                    final List<ClientResponse.Details.Error> errors = clientResponse.getErrors();
                    LOG.error("Some of the metric points failed with errors: [{}]", clientResponse);
                    if (errors != null && !errors.isEmpty()) {

                        Set<Tuple> failedTuples = new HashSet<>();
                        for (ClientResponse.Details.Error error : errors) {
                            final Tuple failedTuple = metricPointsWithTuple.get(error.getDatapoint());
                            if (failedTuple != null) {
                                failedTuples.add(failedTuple);
                            }
                        }

                        for (Tuple batchedTuple : batchHelper.getBatchTuples()) {
                            if (failedTuples.contains(batchedTuple)) {
                                collector.fail(batchedTuple);
                            } else {
                                collector.ack(batchedTuple);
                            }
                        }

                    } else {
                        throw new RuntimeException("Some of the metric points failed with details: " + errors);
                    }
                } else {
                    LOG.debug("Acknowledging batched tuples");
                    batchHelper.ack();
                }
                metricPointsWithTuple.clear();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
            metricPointsWithTuple.clear();
        }
    }

    private List<OpenTsdbMetricDatapoint> getMetricPoints(Tuple tuple) {
        List<OpenTsdbMetricDatapoint> metricDataPoints = new ArrayList<>();
        for (ITupleOpenTsdbDatapointMapper tupleOpenTsdbDatapointMapper : tupleOpenTsdbDatapointMappers) {
            metricDataPoints.add(tupleOpenTsdbDatapointMapper.getMetricPoint(tuple));
        }

        return metricDataPoints;
    }

    @Override
    public void cleanup() {
        openTsdbClient.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this is a sink and no result to emit.
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalInSeconds);
    }
}
