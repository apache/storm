/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.starter.trident;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.CombinerValueUpdater;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.testing.MemoryMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugMemoryMapState<T> extends MemoryMapState<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DebugMemoryMapState.class);

    private int updateCount = 0;

    public DebugMemoryMapState(String id) {
        super(id);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        print(keys, updaters);
        if ((updateCount++ % 5) == 0) {
            LOG.error("Throwing FailedException");
            throw new FailedException("Enforced State Update Fail. On retrial should replay the exact same batch.");
        }
        return super.multiUpdate(keys, updaters);
    }

    private void print(List<List<Object>> keys, List<ValueUpdater> updaters) {
        for (int i = 0; i < keys.size(); i++) {
            ValueUpdater valueUpdater = updaters.get(i);
            Object arg = ((CombinerValueUpdater) valueUpdater).getArg();
            LOG.info("updateCount = {}, keys = {} => updaterArgs = {}", updateCount, keys.get(i), arg);
        }
    }

    public static class Factory implements StateFactory {
        String id;

        public Factory() {
            id = UUID.randomUUID().toString();
        }

        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new DebugMemoryMapState(id + partitionIndex);
        }
    }
}
