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
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.tuple.Fields;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created by pbrahmbhatt on 10/29/15.
 */
public class JdbcLookupBoltTest {

    @Test
    public void testValidation() {
        ConnectionProvider provider = new HikariCPConnectionProvider(new HashMap<>());
        JdbcLookupMapper mapper = new SimpleJdbcLookupMapper(new Fields("test"), Lists.newArrayList(new Column<String>("test", 0)));
        String selectQuery = "select * from dual";
        expectIllegalArgs(null, selectQuery, mapper);
        expectIllegalArgs(provider, null, mapper);
        expectIllegalArgs(provider, selectQuery, null);
    }

    private void expectIllegalArgs(ConnectionProvider provider, String selectQuery, JdbcLookupMapper mapper) {
        assertThrows(IllegalArgumentException.class, () -> new JdbcLookupBolt(provider, selectQuery, mapper));
    }

}
