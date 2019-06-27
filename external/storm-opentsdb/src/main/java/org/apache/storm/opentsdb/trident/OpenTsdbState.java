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

package org.apache.storm.opentsdb.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.opentsdb.OpenTsdbMetricDatapoint;
import org.apache.storm.opentsdb.bolt.ITupleOpenTsdbDatapointMapper;
import org.apache.storm.opentsdb.client.ClientResponse;
import org.apache.storm.opentsdb.client.OpenTsdbClient;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trident {@link State} implementation for OpenTSDB.
 */
public class OpenTsdbState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTsdbState.class);

    private final Map<String, Object> conf;
    private final OpenTsdbClient.Builder openTsdbClientBuilder;
    private final Iterable<? extends ITupleOpenTsdbDatapointMapper> tupleMetricPointMappers;
    private OpenTsdbClient openTsdbClient;

    public OpenTsdbState(Map<String, Object> conf,
            OpenTsdbClient.Builder openTsdbClientBuilder,
            Iterable<? extends ITupleOpenTsdbDatapointMapper> tupleMetricPointMappers) {
        this.conf = conf;
        this.openTsdbClientBuilder = openTsdbClientBuilder;
        this.tupleMetricPointMappers = tupleMetricPointMappers;
    }

    public void prepare() {
        openTsdbClient = openTsdbClientBuilder.build();
    }

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    public void update(List<TridentTuple> tridentTuples, TridentCollector collector) {
        try {
            List<OpenTsdbMetricDatapoint> metricDataPoints = new ArrayList<>();
            for (TridentTuple tridentTuple : tridentTuples) {
                for (ITupleOpenTsdbDatapointMapper tupleOpenTsdbDatapointMapper : tupleMetricPointMappers) {
                    metricDataPoints.add(tupleOpenTsdbDatapointMapper.getMetricPoint(tridentTuple));
                }
            }
            final ClientResponse.Details details = openTsdbClient.writeMetricPoints(metricDataPoints);

            if (details != null && (details.getFailed() > 0)) {
                final String errorMsg = "Failed in writing metrics to TSDB with details: " + details;
                LOG.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }

        } catch (Exception e) {
            collector.reportError(e);
            throw new FailedException(e);
        }

    }
}
