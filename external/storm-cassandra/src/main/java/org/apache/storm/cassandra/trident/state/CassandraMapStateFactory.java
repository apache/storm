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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.TransactionalMap;

import java.util.Map;

/**
 * A StateFactory implementation that creates a MapState backed by CassandraBackingMap.
 *
 * The statefactory supports opaque, transactional and non-transactional configurations.
 * Optionally, the backing map can be wrapped in a {@link CachedMap} by specifying {@link #withCache} (off by default).
 *
 */
public class CassandraMapStateFactory implements StateFactory {

    private final StateType stateType;
    private final CassandraBackingMap.Options options;
    private int cacheSize;
    private Map cassandraConfig;

    private CassandraMapStateFactory(StateType stateType, CassandraBackingMap.Options options, Map cassandraConfig) {
        this.stateType = stateType;
        this.options = options;
        this.cassandraConfig = cassandraConfig;
    }

    public static CassandraMapStateFactory opaque(CassandraBackingMap.Options options, Map cassandraConfig) {
        return new CassandraMapStateFactory(StateType.OPAQUE, options, cassandraConfig);
    }

    public static CassandraMapStateFactory transactional(CassandraBackingMap.Options options, Map cassandraConfig) {
        return new CassandraMapStateFactory(StateType.TRANSACTIONAL, options, cassandraConfig);
    }

    public static CassandraMapStateFactory nonTransactional(CassandraBackingMap.Options options, Map cassandraConfig) {
        return new CassandraMapStateFactory(StateType.NON_TRANSACTIONAL, options, cassandraConfig);
    }

    public CassandraMapStateFactory withCache(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {

        CassandraBackingMap cassandraBackingMap = new CassandraBackingMap(cassandraConfig, options);
        cassandraBackingMap.prepare();

        IBackingMap backingMap = cacheSize > 0
                ? new CachedMap<>(cassandraBackingMap, cacheSize)
                : cassandraBackingMap;

        MapState<?> mapState;

        switch (stateType) {
            case OPAQUE:
                mapState = OpaqueMap.build((IBackingMap<OpaqueValue>) backingMap);
                break;

            case TRANSACTIONAL:
                mapState = TransactionalMap.build((IBackingMap<TransactionalValue>)backingMap);
                break;

            case NON_TRANSACTIONAL:
                mapState = NonTransactionalMap.build(backingMap);
                break;

            default:
                throw new IllegalArgumentException("Invalid state provided " + stateType);
        }

        return mapState;

    }
}
