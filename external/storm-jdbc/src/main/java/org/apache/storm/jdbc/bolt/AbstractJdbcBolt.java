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

import java.sql.DriverManager;
import java.util.Map;
import org.apache.commons.lang.Validate;
import org.apache.storm.Config;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJdbcBolt extends BaseTickTupleAwareRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(
        AbstractJdbcBolt.class);

    static {
        /*
         * Load DriverManager first to avoid any race condition between
         * DriverManager static initialization block and specific driver class's
         * static initialization block. e.g. PhoenixDriver
         *
         * We should take this workaround since prepare() method is synchronized
         * but an worker can initialize multiple AbstractJdbcBolt instances and
         * they would make race condition.
         *
         * We just need to ensure that DriverManager class is always initialized
         * earlier than provider so below line should be called first
         * than initializing provider.
         */
        DriverManager.getDrivers();
    }

    protected OutputCollector collector;
    protected transient JdbcClient jdbcClient;
    protected String configKey;
    protected Integer queryTimeoutSecs;
    protected ConnectionProvider connectionProvider;

    /**
     * Constructor.
     * <p/>
     * @param connectionProviderParam database connection provider
     */
    public AbstractJdbcBolt(final ConnectionProvider connectionProviderParam) {
        Validate.notNull(connectionProviderParam);
        this.connectionProvider = connectionProviderParam;
    }

    /**
     * Subclasses should call this to ensure output collector and connection
     * provider are set up, and finally jdbcClient is initialized properly.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public void prepare(final Map<String, Object> map, final TopologyContext topologyContext,
                        final OutputCollector outputCollector) {
        this.collector = outputCollector;

        connectionProvider.prepare();

        if (queryTimeoutSecs == null) {
            String msgTimeout = map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)
                                   .toString();
            queryTimeoutSecs = Integer.parseInt(msgTimeout);
        }

        this.jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
    }

    /**
     * Cleanup.
     * <p/>
     * Subclasses should call this to ensure connection provider can be
     * also cleaned up.
     */
    @Override
    public void cleanup() {
        connectionProvider.cleanup();
    }
}
