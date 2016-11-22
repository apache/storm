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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An IBackingState implementation for Cassandra.
 *
 * The implementation stores state as a binary blob in cassandra using a {@link Serializer}.
 * It supports Opaque, Transactional and NonTransactional states, given a matching serializer.
 *
 * Configuration is done with three separate constructs:
 *  - One tuple mapper for multiGet, which should map keys to a select statement and return {@link Values}.
 *  - One state mapper, which maps the state to/from a {@link Values} representation, which is used for binding.
 *  - One tuple mapper for multiPut, which should map {@link Values} to an INSERT or UPDATE statement.
 *
 * {@link #multiPut(List, List)} uses Cassandra batch statements (if so configured).
 * {@link #multiGet(List)} queries Cassandra asynchronously, with a default parallelism of 500.
 *
 * @param <T>
 */
public class CassandraBackingMap<T> implements IBackingMap<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraBackingMap.class);

    private final Map conf;
    private final Options<T> options;
    private final Fields allFields;

    private SimpleClient client;
    private Session session;
    private AyncCQLResultSetValuesMapper cqlResultSetValuesMapper;


    protected CassandraBackingMap(Map conf, Options<T> options) {
        this.conf = conf;
        this.options = options;
        List<String> allFields = options.keyFields.toList();
        allFields.addAll(options.stateMapper.getStateFields().toList());
        this.allFields = new Fields(allFields);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        LOG.debug("multiGet fetching {} values.", keys.size());
        List<Statement> selects = new ArrayList<>();
        List<ITuple> keyTuples = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            SimpleTuple keyTuple = new SimpleTuple(options.keyFields, keys.get(i));
            List<Statement> mappedStatements = options.multiGetCqlStatementMapper.map(conf, session, keyTuple);
            if (mappedStatements.size() > 1) {
                throw new IllegalArgumentException("Only one statement per map state item is supported.");
            }
            selects.add(mappedStatements.size() == 1 ? mappedStatements.get(0) : null);
            keyTuples.add(keyTuple);
        }

        List<List<Values>> batchRetrieveResult = cqlResultSetValuesMapper
                .map(session, selects, keyTuples);

        List<T> states = new ArrayList<>();
        for (List<Values> values : batchRetrieveResult) {
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
            statements.addAll(options.multiPutCqlStatementMapper.map(conf, session, tuple));
        }

        try {
            if (options.batchingType != null) {
                BatchStatement batchStatement = new BatchStatement(options.batchingType);
                batchStatement.addAll(statements);
                session.execute(batchStatement);
            } else {
                for (Statement statement : statements) {
                    session.execute(statement);
                }
            }
        } catch (Exception e) {
            LOG.warn("Batch write operation failed: {}", e.getMessage());
            throw new FailedException(e);
        }
    }

    public void prepare() {
        LOG.info("Preparing state for " + options.toString());
        Preconditions.checkNotNull(options.multiGetCqlStatementMapper, "CassandraBackingMap.Options should have multiGetCqlStatementMapper");
        Preconditions.checkNotNull(options.multiPutCqlStatementMapper, "CassandraBackingMap.Options should have multiPutCqlStatementMapper");
        client = options.clientProvider.getClient(conf);
        session = client.connect();
        this.cqlResultSetValuesMapper = new TridentAyncCQLResultSetValuesMapper(options.stateMapper.getStateFields(), options.maxParallelism);
    }

    public static final class Options<T> implements Serializable {
        private final SimpleClientProvider clientProvider;
        private Fields keyFields;
        private StateMapper stateMapper;
        private BatchStatement.Type batchingType;
        private CQLStatementTupleMapper multiGetCqlStatementMapper;
        private CQLStatementTupleMapper multiPutCqlStatementMapper;
        private Integer maxParallelism = 500;

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

        public Options<T> withNonTransactionalJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, new JSONNonTransactionalSerializer());
            return this;
        }

        public Options<T> withNonTransactionalBinaryState(String fieldName, Serializer<T> serializer) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, serializer);
            return this;
        }

        public Options<T> withTransactionalJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, new JSONTransactionalSerializer());
            return this;
        }

        public Options<T> withTransactionalBinaryState(String fieldName, Serializer<TransactionalValue<T>> serializer) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, serializer);
            return this;
        }

        public Options<T> withOpaqueJSONBinaryState(String fieldName) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, new JSONOpaqueSerializer());
            return this;
        }

        public Options<T> withOpaqueBinaryState(String fieldName, Serializer<OpaqueValue<T>> serializer) {
            this.stateMapper = new SerializerStateMapper<>(fieldName, serializer);
            return this;
        }

        public Options<T> withMultiGetCQLStatementMapper(CQLStatementTupleMapper multiGetCqlStatementMapper) {
            this.multiGetCqlStatementMapper = multiGetCqlStatementMapper;
            return this;
        }

        public Options<T> withMultiPutCQLStatementMapper(CQLStatementTupleMapper multiPutCqlStatementMapper) {
            this.multiPutCqlStatementMapper = multiPutCqlStatementMapper;
            return this;
        }

        public Options<T> withBatching(BatchStatement.Type batchingType) {
            this.batchingType = batchingType;
            return this;
        }

        public Options<T> withMaxParallelism(Integer maxParallelism) {
            this.maxParallelism = maxParallelism;
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s: [keys: %s, StateMapper: %s, getMapper: %s, putMapper: %s, batchingType: %s, maxParallelism: %d",
                    this.getClass().getSimpleName(),
                    keyFields,
                    stateMapper,
                    multiGetCqlStatementMapper,
                    multiPutCqlStatementMapper,
                    batchingType,
                    maxParallelism
            );
        }
    }

}
