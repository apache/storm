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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaStateStoreTest {

    private KafkaTestBroker testBroker;
    private KafkaStateStore stateStore;

    @Before
    public void setUp() throws Exception {
        String testTopic = "testTopic";

        TestingServer server = new TestingServer();
        testBroker = new KafkaTestBroker(server, 0);
        String connectionString = server.getConnectString();

        Properties props = new Properties();
        props.put("metadata.broker.list", testBroker.getBrokerConnectionString());
        Producer<byte[], byte[]> p = new Producer<>(new ProducerConfig(props));
        KeyedMessage<byte[], byte[]> msg = new KeyedMessage<>(testTopic, "test message".getBytes());
        p.send(msg);

        ZkHosts hosts = new ZkHosts(connectionString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, testTopic, "/", "testConsumerGroup");

        Map stormConf = new HashMap();

        stateStore = new KafkaStateStore(stormConf, spoutConfig);
    }

    @After
    public void shutdown() throws Exception {
        testBroker.shutdown();
    }

    @Test
    public void testStoreReadWrite() {
        Partition testPartition = new Partition(new Broker("localhost", testBroker.getPort()), "testTopic", 0);

        Map broker = ImmutableMap.of("host", "kafka.sample.net", "port", 9100L);
        Map topology = ImmutableMap.of("id", "fce905ff-25e0 -409e-bc3a-d855f 787d13b", "name", "Test Topology");
        Map testState = ImmutableMap.of("broker", broker, "offset", 4285L, "partition", 1L, "topic", "testTopic", "topology", topology);

        stateStore.writeState(testPartition, testState);
        Map<Object, Object> state = stateStore.readState(testPartition);

        assertEquals("kafka.sample.net", ((Map)state.get("broker")).get("host"));
        assertEquals(9100L, ((Map)state.get("broker")).get("port"));
        assertEquals(4285L, state.get("offset"));
        assertEquals(1L, state.get("partition"));
        assertEquals(4285L, state.get("offset"));
        assertEquals("testTopic", state.get("topic"));
        assertEquals("fce905ff-25e0 -409e-bc3a-d855f 787d13b", ((Map) state.get("topology")).get("id"));
        assertEquals("Test Topology", ((Map)state.get("topology")).get("name"));
    }
}
