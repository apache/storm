/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.trident.state;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.storm.cassandra.client.SimpleClient;
import org.apache.storm.cassandra.client.SimpleClientProvider;
import org.apache.storm.cassandra.query.AyncCQLResultSetValuesMapper;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IBackingState implementation for Cassandra.
 *
 * <p>The implementation stores state as a binary blob in cassandra using a {@link Serializer}.
 * It supports Opaque, Transactional and NonTransactional states, given a matching serializer.
 *
 * <p>Configuration is done with three separate constructs:
 *  - One tuple mapper for multiGet, which should map keys to a select statement and return {@link Values}.
 *  - One state mapper, which maps the state to/from a {@link Values} representation, which is used for binding.
 *  - One tuple mapper for multiPut, which should map {@link Values} to an INSERT or UPDATE statement.
 *
 * <p>{@link #multiPut(List, List)} updates Cassandra with parallel statements.
 * {@link #multiGet(List)} queries Cassandra with parallel statements.
 *
 * <p>Parallelism defaults to half the maximum requests per host, either local or remote whichever is
 * lower. The driver defaults to 256 for remote hosts and 1024 for local hosts, so the default value is 128
 * unless the driver is configured otherwise.
 */
public class CassandraBackingMap<T> implements IBackingMap<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraBackingMap.class);

    private final Map<String, Object> conf;
    private final Options<T> options;
    private final Fields allFields;

    private SimpleClient client;
    private Session session;
    private AyncCQLResultSetValuesMapper getResultMapper;
    private AyncCQLResultSetValuesMapper putResultMapper;
    private Semaphore throttle;


    protected CassandraBackingMap(Map<String, Object> conf, Options<T> options) {
        this.conf = conf;
        this.options = options;
        List<String> allFields = options.keyFields.toList();
        allFields.addAll(options.stateMapper.getStateFields().toList());
        this.allFields = new Fields(allFields);
    }

    public void prepare() {
        LOG.info("Preparing state for {}", options.toString());
        Preconditions.checkNotNull(options.getMapper, "CassandraBackingMap.Options should have getMapper");
        Preconditions.checkNotNull(options.putMapper, "CassandraBackingMap.Options should have putMapper");
        client = options.clientProvider.getClient(conf);
        session = client.connect();
        if (options.maxParallelism == null || options.maxParallelism <= 0) {
            PoolingOptions po = session.getCluster().getConfiguration().getPoolingOptions();
            Integer maxRequestsPerHost = Math.min(
                po.getMaxConnectionsPerHost(HostDistance.LOCAL) * po.getMaxRequestsPerConnection(HostDistance.LOCAL),
                po.getMaxConnectionsPerHost(HostDistance.REMOTE) * po.getMaxRequestsPerConnection(HostDistance.REMOTE)
            );
            options.maxParallelism = maxRequestsPerHost / 2;
            LOG.info("Parallelism default set to {}", options.maxParallelism);
        }
        throttle = new Semaphore(options.maxParallelism, false);
        this.getResultMapper = new TridentAyncCQLResultSetValuesMapper(options.stateMapper.getStateFields(), throttle);
        this.putResultMapper = new TridentAyncCQLResultSetValuesMapper(null, throttle);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        LOG.debug("multiGet fetching {} values.", keys.size());
        List<Statement> selects = new ArrayList<>();
        List<ITuple> keyTuples = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            SimpleTuple keyTuple = new SimpleTuple(options.keyFields, keys.get(i));
            List<Statement> mappedStatements = options.getMapper.map(conf, session, keyTuple);
            if (mappedStatements.size() > 1) {
                throw new IllegalArgumentException("Only one statement per map state item is supported.");
            }
            selects.add(mappedStatements.size() == 1 ? mappedStatements.get(0) : null);
            keyTuples.add(keyTuple);
        }

        List<List<Values>> results = getResultMapper
            .map(session, selects, keyTuples);

        List<T> states = new ArrayList<>();
        for (List<Values> values : results) {
            T state = (T) options.stateMapper.fromValues(values);
            states.add(state);
        }

        return states;

    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        LOG.debug("multiPut writing {} values.", keys.size());

        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            Values stateValues = options.stateMapper.toValues(values.get(i));
            SimpleTuple tuple = new SimpleTuple(allFields, keys.get(i), stateValues);
            statements.addAll(options.putMapper.map(conf, session, tuple));
        }

        try {
            putResultMapper.map(session, statements, null);
        } catch (Exception e) {
            LOG.warn("Write operation failed: {}", e.getMessage());
            throw new FailedException(e);
        }
    }

    public static final class Options<T> implements Serializable {
        private final SimpleClientProvider clientProvider;
        private Fields keyFields;
        private StateMapper stateMapper;
        private CQLStatementTupleMapper getMapper;
        private CQLStatementTupleMapper putMapper;
        private Integer maxParallelism = 128;

        public Options(SimpleClientProvider clientProvider) {
            this.clientProvider = clientProvider;
        }

        public Options<T> withKeys(Fields keyFields) {
            this.keyFields = keyFields;
            return this;
        }

        public Options<T> withStateMapper(StateMapper<T> stateMapper) {
            this.stateMapper = stateMapper;
            return this;
        }

        @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
        public Options<T> withNonTransactionalJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, new JSONNonTransactionalSerializer());
            return this;
        }

        public Options<T> withNonTransactionalBinaryState(String fieldName, Serializer<T> serializer) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, serializer);
            return this;
        }

        @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
        public Options<T> withTransactionalJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, new JSONTransactionalSerializer());
            return this;
        }

        public Options<T> withTransactionalBinaryState(String fieldName, Serializer<TransactionalValue<T>> serializer) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, serializer);
            return this;
        }

        @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
        public Options<T> withOpaqueJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, new JSONOpaqueSerializer());
            return this;
        }

        public Options<T> withOpaqueBinaryState(String fieldName, Serializer<OpaqueValue<T>> serializer) {
            this.stateMapper = new SerializedStateMapper<>(fieldName, serializer);
            return this;
        }

        public Options<T> withGetMapper(CQLStatementTupleMapper getMapper) {
            this.getMapper = getMapper;
            return this;
        }

        public Options<T> withPutMapper(CQLStatementTupleMapper putMapper) {
            this.putMapper = putMapper;
            return this;
        }

        public Options<T> withMaxParallelism(Integer maxParallelism) {
            this.maxParallelism = maxParallelism;
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s: [keys: %s, StateMapper: %s, getMapper: %s, putMapper: %s, maxParallelism: %d",
                                 this.getClass().getSimpleName(),
                                 keyFields,
                                 stateMapper,
                                 getMapper,
                                 putMapper,
                                 maxParallelism
            );
        }
    }

}
