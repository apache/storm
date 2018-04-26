/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hbase.state;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link HBaseKeyValueStateProvider}
 */
public class HBaseKeyValueStateProviderTest {

    @Test
    public void testConfigHBaseConfigKeyIsEmpty() throws Exception {
        HBaseKeyValueStateProvider provider = new HBaseKeyValueStateProvider();
        Map<String, String> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{\"keyClass\":\"String\", \"valueClass\":\"String\"," +
                                                             " \"tableName\": \"table\", \"columnFamily\": \"cf\"}");

        try {
            HBaseKeyValueStateProvider.StateConfig config = provider.getStateConfig(stormConf);
            fail("IllegalArgumentException is expected here.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("hbaseConfigKey"));
        }
    }

    @Test
    public void testConfigTableNameIsEmpty() throws Exception {
        HBaseKeyValueStateProvider provider = new HBaseKeyValueStateProvider();
        Map<String, String> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{\"keyClass\":\"String\", \"valueClass\":\"String\"," +
                                                             " \"hbaseConfigKey\": \"hbaseConfKey\", \"columnFamily\": \"cf\"}");

        try {
            HBaseKeyValueStateProvider.StateConfig config = provider.getStateConfig(stormConf);
            fail("IllegalArgumentException is expected here.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("tableName"));
        }
    }

    @Test
    public void testConfigColumnFamilyIsEmpty() throws Exception {
        HBaseKeyValueStateProvider provider = new HBaseKeyValueStateProvider();
        Map<String, String> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{\"keyClass\":\"String\", \"valueClass\":\"String\"," +
                                                             " \"hbaseConfigKey\": \"hbaseConfKey\", \"tableName\": \"table\"}");

        try {
            HBaseKeyValueStateProvider.StateConfig config = provider.getStateConfig(stormConf);
            fail("IllegalArgumentException is expected here.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("columnFamily"));
        }
    }

    @Test
    public void testValidProviderConfig() throws Exception {
        HBaseKeyValueStateProvider provider = new HBaseKeyValueStateProvider();
        Map<String, String> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{\"keyClass\":\"String\", \"valueClass\":\"String\"," +
                                                             " \"hbaseConfigKey\": \"hbaseConfKey\", \"tableName\": \"table\"," +
                                                             " \"columnFamily\": \"columnFamily\"}");

        HBaseKeyValueStateProvider.StateConfig config = provider.getStateConfig(stormConf);
        assertEquals("String", config.keyClass);
        assertEquals("String", config.valueClass);
        assertEquals("hbaseConfKey", config.hbaseConfigKey);
        assertEquals("table", config.tableName);
        assertEquals("columnFamily", config.columnFamily);
    }
}