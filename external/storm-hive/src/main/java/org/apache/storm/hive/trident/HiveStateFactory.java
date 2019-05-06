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

package org.apache.storm.hive.trident;

import java.util.Map;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveStateFactory.class);
    private HiveOptions options;

    public HiveStateFactory() {}

    /**
     * The options for connecting to Hive.
     */
    public HiveStateFactory withOptions(HiveOptions options) {
        if (options.getTickTupleInterval() != HiveOptions.DEFAULT_TICK_TUPLE_INTERVAL_SECS) {
            LOG.error("Tick tuple interval will be ignored for trident."
                    + " The Hive writers are flushed after each batch.");
        }
        this.options = options;
        return this;
    }

    @Override
    public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        HiveState state = new HiveState(this.options);
        state.prepare(conf, metrics, partitionIndex, numPartitions);
        return state;
    }
}
