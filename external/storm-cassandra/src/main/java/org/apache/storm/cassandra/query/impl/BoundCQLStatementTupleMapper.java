/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.query.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.Column;
import org.apache.storm.cassandra.query.ContextQuery;
import org.apache.storm.cassandra.query.CqlMapper;
import org.apache.storm.tuple.ITuple;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class BoundCQLStatementTupleMapper implements CQLStatementTupleMapper {

    private final ContextQuery contextQuery;

    private final CqlMapper mapper;
    private final RoutingKeyGenerator rkGenerator;
    private final PreparedStatementBinder binder;
    private Map<String, PreparedStatement> cache = new HashMap<>();

    /**
     * Creates a new {@link BoundCQLStatementTupleMapper} instance.
     */
    public BoundCQLStatementTupleMapper(ContextQuery contextQuery, CqlMapper mapper, RoutingKeyGenerator rkGenerator,
                                        PreparedStatementBinder binder) {
        Preconditions.checkNotNull(contextQuery, "ContextQuery must not be null");
        Preconditions.checkNotNull(mapper, "Mapper should not be null");
        this.contextQuery = contextQuery;
        this.mapper = mapper;
        this.rkGenerator = rkGenerator;
        this.binder = binder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Statement<?>> map(Map<String, Object> config, CqlSession session, ITuple tuple) {
        final ArrayList<Statement<?>> ret = new ArrayList<>();
        final List<Column> columns = mapper.map(tuple);

        final String query = contextQuery.resolves(config, tuple);

        PreparedStatement statement = getPreparedStatement(session, query);
        BoundStatement boundStatement = this.binder.apply(statement, columns);
        ret.add(boundStatement);
        if (hasRoutingKeys()) {
            List<ByteBuffer> keys = rkGenerator.getRoutingKeys(tuple);
            if (keys.size() == 1) {
                boundStatement.setRoutingKey(keys.get(0));
            } else {
                boundStatement.setRoutingKey(keys.toArray(new ByteBuffer[keys.size()]));
            }
        }
        return ret;
    }

    private boolean hasRoutingKeys() {
        return rkGenerator != null;
    }

    /**
     * Get or prepare a statement using the specified session and the query.
     * *
     * @param session The cassandra session.
     * @param query The CQL query to prepare.
     */
    private PreparedStatement getPreparedStatement(CqlSession session, String query) {
        PreparedStatement statement = cache.get(query);
        if (statement == null) {
            statement = session.prepare(query);
            cache.put(query, statement);
        }
        return statement;
    }
}
