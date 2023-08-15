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

import com.google.common.collect.Lists;
import java.util.HashMap;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created by pbrahmbhatt on 10/29/15.
 */
public class JdbcInsertBoltTest {

    @Test
    public void testValidation() {
        ConnectionProvider provider = new HikariCPConnectionProvider(new HashMap<>());
        JdbcMapper mapper = new SimpleJdbcMapper(Lists.newArrayList(new Column<String>("test", 0)));
        expectNullPointerException(null, mapper);
        expectNullPointerException(provider, null);

        assertThrows(IllegalArgumentException.class, () -> {
            JdbcInsertBolt bolt = new JdbcInsertBolt(provider, mapper);
            bolt.withInsertQuery("test");
            bolt.withTableName("test");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            JdbcInsertBolt bolt = new JdbcInsertBolt(provider, mapper);
            bolt.withTableName("test");
            bolt.withInsertQuery("test");
        });
    }

    private void expectNullPointerException(ConnectionProvider provider, JdbcMapper mapper) {
        assertThrows(NullPointerException.class, () -> new JdbcInsertBolt(provider, mapper));
    }

}
