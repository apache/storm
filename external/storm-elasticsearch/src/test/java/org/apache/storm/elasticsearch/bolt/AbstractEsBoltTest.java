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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.bolt;

import com.google.common.testing.NullPointerTester;

import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractEsBoltTest<Bolt extends AbstractEsBolt> {

    protected static Config config = new Config();
    protected static final String documentId = UUID.randomUUID().toString();
    protected static final String index = "index";
    protected static final String source = "{\"user\":\"user1\"}";
    protected static final String type = "type";

    @Mock
    protected OutputCollector outputCollector;

    protected Bolt bolt;

    @BeforeEach
    public void createBolt() throws Exception {
        bolt = createBolt(esConfig());
        bolt.prepare(config, null, outputCollector);
    }

    protected abstract Bolt createBolt(EsConfig esConfig);

    protected EsConfig esConfig() {
        return new EsConfig();
    }

    @AfterEach
    public void cleanupBolt() throws Exception {
        bolt.cleanup();
    }

    @Test
    public void constructorsThrowOnNull() throws Exception {
        new NullPointerTester().setDefault(EsConfig.class, esConfig()).testAllPublicConstructors(getBoltClass());
    }

    @Test
    public void getEndpointThrowsOnNull() throws Exception {
        Method getEndpointMethod = AbstractEsBolt.class.getDeclaredMethod("getEndpoint", String.class, String.class, String.class);
        new NullPointerTester().setDefault(String.class, "test").testMethodParameter(null, getEndpointMethod, 0);
    }

    protected abstract Class<Bolt> getBoltClass();
}
