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

import java.util.List;
import java.util.Map;

import org.apache.storm.opentsdb.bolt.ITupleOpenTsdbDatapointMapper;
import org.apache.storm.opentsdb.client.OpenTsdbClient;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

/**
 * Trident {@link StateFactory} implementation for OpenTSDB.
 */
public class OpenTsdbStateFactory implements StateFactory {

    private OpenTsdbClient.Builder builder;
    private final List<? extends ITupleOpenTsdbDatapointMapper> tridentTupleOpenTsdbDatapointMappers;

    public OpenTsdbStateFactory(OpenTsdbClient.Builder builder,
            List<? extends ITupleOpenTsdbDatapointMapper> tridentTupleOpenTsdbDatapointMappers) {
        this.builder = builder;
        this.tridentTupleOpenTsdbDatapointMappers = tridentTupleOpenTsdbDatapointMappers;
    }

    @Override
    public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        final OpenTsdbState openTsdbState = new OpenTsdbState(conf, builder, tridentTupleOpenTsdbDatapointMappers);
        openTsdbState.prepare();

        return openTsdbState;
    }
}
