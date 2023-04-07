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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.cassandra.client.SimpleClient;
import org.apache.storm.cassandra.client.SimpleClientProvider;
import org.apache.storm.cassandra.query.CQLResultSetValuesMapper;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraState.class);

    private final Map<String, Object> conf;
    private final Options options;

    private CqlSession session;
    private SimpleClient client;

    protected CassandraState(Map<String, Object> conf, Options options) {
        this.conf = conf;
        this.options = options;
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is no operation");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is no operation");
    }

    public void prepare() {
        Preconditions.checkNotNull(options.cqlStatementTupleMapper, "CassandraState.Options should have cqlStatementTupleMapper");

        client = options.clientProvider.getClient(conf);
        session = client.connect();
    }

    public void cleanup() {
        try {
            session.close();
        } catch (Exception e) {
            LOG.warn("Error occurred while closing Session", e);
        } finally {
            client.close();
        }
    }

    public void updateState(List<TridentTuple> tuples, final TridentCollector collector) {

        List<Statement> statements = new ArrayList<>();
        for (TridentTuple tuple : tuples) {
            statements.addAll(options.cqlStatementTupleMapper.map(conf, session, tuple));
        }

        try {
            if (options.batchingType != null) {
                BatchStatement batchStatement = BatchStatement.newInstance(options.batchingType);
                statements.forEach(statement -> batchStatement.add((BatchableStatement) statement));
                session.execute(batchStatement);
            } else {
                for (Statement statement : statements) {
                    session.execute(statement);
                }
            }
        } catch (Exception e) {
            LOG.warn("Batch write operation is failed.");
            collector.reportError(e);
            throw new FailedException(e);
        }

    }

    public List<List<Values>> batchRetrieve(List<TridentTuple> tridentTuples) {
        Preconditions.checkNotNull(options.cqlResultSetValuesMapper, "CassandraState.Options should have cqlResultSetValuesMapper");

        List<List<Values>> batchRetrieveResult = new ArrayList<>();
        try {
            for (TridentTuple tridentTuple : tridentTuples) {
                List<? extends Statement> statements = options.cqlStatementTupleMapper.map(conf, session, tridentTuple);
                for (Statement statement : statements) {
                    List<List<Values>> values = options.cqlResultSetValuesMapper.map(session, statement, tridentTuple);
                    batchRetrieveResult.addAll(values);
                }
            }
        } catch (Exception e) {
            LOG.warn("Batch retrieve operation is failed", e);
            throw new FailedException(e);
        }
        return batchRetrieveResult;
    }

    public static final class Options implements Serializable {
        private final SimpleClientProvider clientProvider;
        private CQLStatementTupleMapper cqlStatementTupleMapper;
        private CQLResultSetValuesMapper cqlResultSetValuesMapper;
        private BatchType batchingType;

        public Options(SimpleClientProvider clientProvider) {
            this.clientProvider = clientProvider;
        }

        @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
        public Options withCQLStatementTupleMapper(CQLStatementTupleMapper cqlStatementTupleMapper) {
            this.cqlStatementTupleMapper = cqlStatementTupleMapper;
            return this;
        }

        @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
        public Options withCQLResultSetValuesMapper(CQLResultSetValuesMapper cqlResultSetValuesMapper) {
            this.cqlResultSetValuesMapper = cqlResultSetValuesMapper;
            return this;
        }

        public Options withBatching(BatchType batchingType) {
            this.batchingType = batchingType;
            return this;
        }

    }

}
