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

package org.apache.storm.jdbc.bolt;

import java.util.List;
import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt for querying from any database.
 */
public class JdbcLookupBolt extends AbstractJdbcBolt {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupBolt.class);

    private String selectQuery;

    private JdbcLookupMapper jdbcLookupMapper;

    public JdbcLookupBolt(ConnectionProvider connectionProvider, String selectQuery, JdbcLookupMapper jdbcLookupMapper) {
        super(connectionProvider);

        Validate.notNull(selectQuery);
        Validate.notNull(jdbcLookupMapper);

        this.selectQuery = selectQuery;
        this.jdbcLookupMapper = jdbcLookupMapper;
    }

    public JdbcLookupBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }

    @Override
    protected void process(Tuple tuple) {
        try {
            List<Column> columns = jdbcLookupMapper.getColumns(tuple);
            List<List<Column>> result = jdbcClient.select(this.selectQuery, columns);

            if (result != null && result.size() != 0) {
                for (List<Column> row : result) {
                    List<Values> values = jdbcLookupMapper.toTuple(tuple, row);
                    for (Values value : values) {
                        collector.emit(tuple, value);
                    }
                }
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        jdbcLookupMapper.declareOutputFields(outputFieldsDeclarer);
    }
}
