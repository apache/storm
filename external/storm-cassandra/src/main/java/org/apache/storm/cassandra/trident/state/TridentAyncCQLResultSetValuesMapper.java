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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.storm.cassandra.executor.AsyncExecutor;
import org.apache.storm.cassandra.executor.AsyncExecutorProvider;
import org.apache.storm.cassandra.executor.AsyncResultHandler;
import org.apache.storm.cassandra.executor.AsyncResultSetHandler;
import org.apache.storm.cassandra.query.AyncCQLResultSetValuesMapper;
import org.apache.storm.topology.FailedException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * A result set mapper implementation which runs requests in parallel and waits for them all to finish.
 */
public class TridentAyncCQLResultSetValuesMapper implements AyncCQLResultSetValuesMapper {
    private final Fields outputDeclaredFields;
    private final Integer maxParallelRequests;

    public TridentAyncCQLResultSetValuesMapper(Fields outputDeclaredFields, Integer maxParallelRequests) {
        this.outputDeclaredFields = outputDeclaredFields;
        this.maxParallelRequests = maxParallelRequests;
    }

    @Override
    public List<List<Values>> map(Session session, List<Statement> statements, final List<ITuple> tuples) {
        AsyncExecutor<Integer> executor = AsyncExecutorProvider.getLocal(session, AsyncResultHandler.NO_OP_HANDLER);
        final List<Integer> indexes = new ArrayList<>();
        final List<List<Values>> results = new ArrayList<>();
        for (int i = 0; i < statements.size(); i++) {
            indexes.add(i);
            results.add(null);
        }
        SettableFuture<List<Integer>> result = executor.execAsync(statements, indexes, 1, new AsyncResultSetHandler<Integer>() {
            @Override
            public void success(Integer index, ResultSet resultSet) {
                if (outputDeclaredFields != null) {
                    List<Values> thisResult = new ArrayList<>();
                    for (Row row : resultSet) {
                        final Values values = new Values();
                        for (String field : outputDeclaredFields) {
                            ITuple tuple = tuples.get(index);
                            if (tuple.contains(field)) {
                                values.add(tuple.getValueByField(field));
                            } else {
                                values.add(row.getObject(field));
                            }
                        }
                        thisResult.add(values);
                    }
                    results.set(index, thisResult);
                }
            }

            @Override
            public void failure(Throwable t, Integer index) {
                // Exceptions are captured and thrown at the end of the batch by the executor
            }

        });

        try {
            // Await all results
            result.get();
        } catch (Exception e) {
            throw new FailedException(e.getMessage(), e);
        }

        return results;
    }

    protected List<Values> handleResult(ResultSet resultSet, ITuple tuple) {
        List<Values> list = new ArrayList<>();
        for (Row row : resultSet) {
            final Values values = new Values();
            for (String field : outputDeclaredFields) {
                if (tuple.contains(field)) {
                    values.add(tuple.getValueByField(field));
                } else {
                    values.add(row.getObject(field));
                }
            }
            list.add(values);
        }
        return list;
    }



}
