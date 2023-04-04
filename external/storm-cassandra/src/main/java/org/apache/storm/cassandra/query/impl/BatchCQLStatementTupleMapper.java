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

package org.apache.storm.cassandra.query.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.tuple.ITuple;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class BatchCQLStatementTupleMapper implements CQLStatementTupleMapper {

    private final List<BatchCQLStatementTupleMapper> mappers;
    private final BatchType type;

    /**
     * Creates a new {@link BatchCQLStatementTupleMapper} instance.
     */
    public BatchCQLStatementTupleMapper(BatchType type, List<BatchCQLStatementTupleMapper> mappers) {
        this.mappers = new ArrayList<>(mappers);
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BatchableStatement<?>> map(Map<String, Object> conf, CqlSession session, ITuple tuple) {
        List<BatchableStatement<?>> ret = new ArrayList<>();
        final BatchStatement batch = BatchStatement.newInstance(this.type);
        for (BatchCQLStatementTupleMapper m : mappers) {
            List<? extends BatchableStatement<?>> statements = m.map(conf, session, tuple);
            statements.forEach(batch::add);
        }
        batch.forEach(ret::add);
        return ret;
    }
}
