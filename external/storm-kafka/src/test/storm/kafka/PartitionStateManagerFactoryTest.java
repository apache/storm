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
package storm.kafka;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PartitionStateManagerFactoryTest {

    PartitionStateManagerFactory factory;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testCustomStateStore() throws Exception {
        Map stormConfig = new HashMap();
        SpoutConfig spoutConfig = new SpoutConfig(null, null, null);
        spoutConfig.stateStore = TestStateStore.class.getName();

        factory = new PartitionStateManagerFactory(stormConfig, spoutConfig);

        PartitionStateManager partitionStateManager = factory.getInstance(null);

        assertEquals(TestStateStore.MAGIC_STATE, partitionStateManager.getState());
    }

    public static class TestStateStore implements StateStore {

        public static final Map<Object, Object> MAGIC_STATE = ImmutableMap.of(new Object(), new Object());

        public TestStateStore(Map stormConf, SpoutConfig spoutConfig) {}

        @Override
        public Map<Object, Object> readState(Partition p) {
            return MAGIC_STATE;
        }

        @Override
        public void writeState(Partition p, Map<Object, Object> state) {

        }

        @Override
        public void close() throws IOException {

        }
    }
}