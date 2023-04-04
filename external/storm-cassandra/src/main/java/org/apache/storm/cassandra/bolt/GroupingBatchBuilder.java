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

package org.apache.storm.cassandra.bolt;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.storm.tuple.Tuple;

public class GroupingBatchBuilder implements Iterable<PairBatchStatementTuples> {

    private int batchSizeRows;

    private List<PairStatementTuple> statements;

    /**
     * Creates a new  {@link GroupingBatchBuilder} instance.
     */
    public GroupingBatchBuilder(int batchSizeRows, List<PairStatementTuple> statements) {
        this.batchSizeRows = batchSizeRows;
        this.statements = statements;
    }

    @Override
    public Iterator<PairBatchStatementTuples> iterator() {
        return build().iterator();
    }

    private Iterable<PairBatchStatementTuples> build() {
        Iterable<List<PairStatementTuple>> partition = Iterables.partition(statements, batchSizeRows);
        return StreamSupport.stream(partition.spliterator(), false).map(l -> {
            final List<Tuple> inputs = new LinkedList<>();
            final BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
            for (PairStatementTuple pair : l) {
                batch.add((BatchableStatement) pair.getStatement());
                inputs.add(pair.getTuple());
            }
            return new PairBatchStatementTuples(inputs, batch);
        }).collect(Collectors.toList());
    }
}
