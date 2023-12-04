/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.redis.bolt;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.redis.util.JedisTestHelper;
import org.apache.storm.redis.util.StubTuple;
import org.apache.storm.redis.util.TupleTestHelper;
import org.apache.storm.redis.util.outputcollector.EmittedTuple;
import org.apache.storm.redis.util.outputcollector.StubOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.GEO;
import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.HASH;
import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.HYPER_LOG_LOG;
import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.SET;
import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.SORTED_SET;
import static org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@Testcontainers
class RedisFilterBoltTest {

    @Container
    public GenericContainer container = new GenericContainer("redis:7.2.3-alpine")
        .withExposedPorts(6379);

    private JedisTestHelper jedisHelper;

    private JedisPoolConfig.Builder configBuilder;
    private StubOutputCollector outputCollector;
    private TopologyContext topologyContext;

    @BeforeEach
    void setup() {
        configBuilder = new JedisPoolConfig.Builder();
        configBuilder
            .setHost(container.getHost())
            .setPort(container.getFirstMappedPort())
            .setTimeout(10);

        outputCollector = new StubOutputCollector();
        topologyContext = mock(TopologyContext.class);
        jedisHelper = new JedisTestHelper(container);
    }

    @AfterEach
    void cleanup() {
        verifyNoMoreInteractions(topologyContext);
        jedisHelper.close();
    }

    /**
     * Smoke test the exists check when the key is NOT found.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_exists_keyNotFound() {
        // Define input key
        final String inputKey = "ThisIsMyKey";

        // Ensure key does not exist in redis
        jedisHelper.delete(inputKey);
        assertFalse(jedisHelper.exists(inputKey), "Sanity check key should not exist");

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(STRING);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the exists check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_exists_keyFound() {
        // Define input key
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.set(inputKey, "some-value");
        assertTrue(jedisHelper.exists(inputKey), "Sanity check key exists.");

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(STRING);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Smoke test the sismember check when the key is NOT found in the set.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_sismember_notMember() {
        // Define input key
        final String setKey = "ThisIsMySet";
        final String inputKey = "ThisIsMyKey";

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(SET, setKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the exists check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_sismember_isMember() {
        // Define input key
        final String setKey = "ThisIsMySet";
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.smember(setKey, inputKey);
        assertTrue(jedisHelper.sismember(setKey, inputKey), "Sanity check, should be a member");

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(SET, setKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Smoke test the hexists check when the key is NOT found in the set.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_hexists_notMember() {
        // Define input key
        final String hashKey = "ThisIsMyHash";
        final String inputKey = "ThisIsMyKey";

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(HASH, hashKey);


        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the hexists check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_hexists_isMember() {
        // Define input key
        final String hashKey = "ThisIsMyHash";
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.hset(hashKey, inputKey, "value");
        assertTrue(jedisHelper.hexists(hashKey, inputKey), "Sanity check, should be a member");

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(HASH, hashKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Smoke test the zrank check when the key is NOT found in the set.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_zrank_notMember() {
        // Define input key
        final String setKey = "ThisIsMySetKey";
        final String inputKey = "ThisIsMyKey";

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(SORTED_SET, setKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the zrank check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_zrank_isMember() {
        // Define input key
        final String setKey = "ThisIsMySetKey";
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.zrank(setKey, 2, inputKey);

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(SORTED_SET, setKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Smoke test the pfcount check when the key is NOT found in the set.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_pfcount_notMember() {
        // Define input key
        final String inputKey = "ThisIsMyKey";

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(HYPER_LOG_LOG);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the pfcount check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_pfcount_isMember() {
        // Define input key
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.pfadd(inputKey, "my value");

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(HYPER_LOG_LOG);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Smoke test the geopos check when the key is NOT found in the set.
     * Expectation is tuple is acked, and nothing is emitted.
     */
    @Test
    void smokeTest_geopos_notMember() {
        // Define input key
        final String geoKey = "ThisIsMyGeoKey";
        final String inputKey = "ThisIsMyKey";

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(GEO, geoKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify the bolt filtered the input tuple.
        verifyTupleFiltered();
    }

    /**
     * Smoke test the geopos check when the key IS found.
     * Expectation is tuple is acked, and tuple is emitted.
     */
    @Test
    void smokeTest_geopos_isMember() {
        // Define input key
        final String geoKey = "ThisIsMyGeoKey";
        final String inputKey = "ThisIsMyKey";

        // Ensure key does exist in redis
        jedisHelper.geoadd(geoKey, 139.731992, 35.709026, inputKey);

        // Create an input tuple
        final Map<String, Object> values = new HashMap<>();
        values.put("key", inputKey);
        values.put("value", "ThisIsMyValue");
        final Tuple tuple = new StubTuple(values);

        final JedisPoolConfig config = configBuilder.build();
        final TestMapper mapper = new TestMapper(GEO, geoKey);

        final RedisFilterBolt bolt = new RedisFilterBolt(config, mapper);
        bolt.prepare(new HashMap<>(), topologyContext, new OutputCollector(outputCollector));
        bolt.process(tuple);

        // Verify Tuple passed through the bolt
        verifyTuplePassed(tuple);
    }

    /**
     * Utility method to help verify that a tuple passed throught hte RedisFilterBolt properly.
     * @param expectedTuple The tuple we expected to pass through the bolt.
     */
    private void verifyTuplePassed(final Tuple expectedTuple) {
        // Verify no errors or failed tuples
        assertTrue(outputCollector.getReportedErrors().isEmpty(), "Should have no reported errors");
        assertTrue(outputCollector.getFailedTuples().isEmpty(), "Should have no failed tuples");

        // We should have a single acked tuple
        assertEquals(1, outputCollector.getAckedTuples().size(), "Should have a single acked tuple");

        // We should have a single emitted tuple.
        assertEquals(1, outputCollector.getEmittedTuples().size(), "Should have a single emitted tuple");

        // Verify the tuple is what we expected
        final EmittedTuple emittedTuple = outputCollector.getEmittedTuples().get(0);
        assertEquals("default", emittedTuple.getStreamId());
        TupleTestHelper.verifyAnchors(emittedTuple, expectedTuple);
        TupleTestHelper.verifyEmittedTuple(emittedTuple, expectedTuple.getValues());
    }

    /**
     * Utility method to help verify that no tuples were passed through the RedisFilterBolt.
     */
    private void verifyTupleFiltered() {
        // Verify no errors or failed tuples
        assertTrue(outputCollector.getReportedErrors().isEmpty(), "Should have no reported errors");
        assertTrue(outputCollector.getFailedTuples().isEmpty(), "Should have no failed tuples");

        // We should have a single acked tuple
        assertEquals(1, outputCollector.getAckedTuples().size(), "Should have a single acked tuple");

        // We should have no emitted tuple.
        assertTrue(outputCollector.getEmittedTuples().isEmpty(), "Should have no emitted tuples");
    }

    /**
     * Test Implementation.
     */
    private static class TestMapper implements RedisFilterMapper {
        private final RedisDataTypeDescription.RedisDataType dataType;
        private final String additionalKey;

        private TestMapper(final RedisDataTypeDescription.RedisDataType dataType) {
            this(dataType, null);
        }

        private TestMapper(final RedisDataTypeDescription.RedisDataType dataType, final String additionalKey) {
            this.dataType = dataType;
            this.additionalKey = additionalKey;
        }

        @Override
        public void declareOutputFields(final OutputFieldsDeclarer declarer) {
            declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("key", "value"));
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return new RedisDataTypeDescription(dataType, additionalKey);
        }

        @Override
        public String getKeyFromTuple(final ITuple tuple) {
            return tuple.getStringByField("key");
        }

        @Override
        public String getValueFromTuple(final ITuple tuple) {
            return tuple.getStringByField("value");
        }
    }
}