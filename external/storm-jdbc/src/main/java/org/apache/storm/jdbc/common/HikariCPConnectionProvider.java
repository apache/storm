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

package org.apache.storm.jdbc.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HikariCPConnectionProvider implements ConnectionProvider {
    private static final Logger LOG = LoggerFactory.getLogger(HikariCPConnectionProvider.class);

    private Map<String, Object> configMap;
    private transient HikariDataSource dataSource;

    public HikariCPConnectionProvider(Map<String, Object> configMap) {
        this.configMap = configMap;
    }

    @Override
    public synchronized void prepare() {
        if (dataSource == null) {
            Properties properties = new Properties();
            properties.putAll(configMap);
            HikariConfig config = new HikariConfig(properties);
            if (properties.containsKey("dataSource.url")) {
                LOG.info("DataSource Url: " + properties.getProperty("dataSource.url"));
            } else if (config.getJdbcUrl() != null) {
                LOG.info("JDBC Url: " + config.getJdbcUrl());
            }
            config.setAutoCommit(false);
            this.dataSource = new HikariDataSource(config);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
