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
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class TopologySpoutLagTest {

    private Properties loadProperties(File file) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return properties;
    }

    @Test
    public void testOnlyKnownConsumerPropertiesArePassedToTheMonitor() throws Exception {
        Map<String, Object> jsonConf = new HashMap<>();
        // Passed to the monitor as command line options instead of via the properties file.
        jsonConf.put("config.topics", "topic");
        jsonConf.put("config.groupid", "group");
        jsonConf.put("config.bootstrap.servers", "broker:9092");
        jsonConf.put("config.security.protocol", "SASL_SSL");
        // Connection settings the monitor needs.
        jsonConf.put("config.sasl.mechanism", "SCRAM-SHA-512");
        jsonConf.put("config.ssl.truststore.location", "/etc/storm/truststore.jks");
        jsonConf.put("config.client.id", "lag-monitor");
        // Properties that would make the monitor load classes named by the topology.
        jsonConf.put("config.key.deserializer", "org.example.MyDeserializer");
        jsonConf.put("config.value.deserializer", "org.example.MyDeserializer");
        jsonConf.put("config.interceptor.classes", "org.example.MyInterceptor");
        jsonConf.put("config.metric.reporters", "org.example.MyReporter");
        jsonConf.put("config.sasl.jaas.config", "org.example.MyLoginModule required;");
        jsonConf.put("config.sasl.login.callback.handler.class", "org.example.MyCallbackHandler");
        // Keys without the config. prefix are not consumer properties at all.
        jsonConf.put("topology.name", "test");

        File file = TopologySpoutLag.createExtraPropertiesFile(jsonConf);
        try {
            Properties properties = loadProperties(file);
            Properties expected = new Properties();
            expected.put("sasl.mechanism", "SCRAM-SHA-512");
            expected.put("ssl.truststore.location", "/etc/storm/truststore.jks");
            expected.put("client.id", "lag-monitor");
            assertEquals(expected, properties);
        } finally {
            file.delete();
        }
    }

    @Test
    public void testNoPropertiesFileWhenNoKnownConsumerPropertiesAreSet() {
        Map<String, Object> jsonConf = new HashMap<>();
        jsonConf.put("config.topics", "topic");
        jsonConf.put("config.groupid", "group");
        jsonConf.put("config.bootstrap.servers", "broker:9092");
        jsonConf.put("config.interceptor.classes", "org.example.MyInterceptor");

        assertNull(TopologySpoutLag.createExtraPropertiesFile(jsonConf));
    }
}
