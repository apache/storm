package storm.kafka;

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

public class KafkaDataSourceTest {

    private KafkaTestBroker testBroker;
    private KafkaDataStore dataStore;

    @Before
    public void setUp() throws Exception {
        String testTopic = "testTopic";

        TestingServer server = new TestingServer();
        testBroker = new KafkaTestBroker(server, 0);
        String connectionString = server.getConnectString();

        Properties props = new Properties();
        props.put("metadata.broker.list", testBroker.getBrokerConnectionString());
        Producer p = new Producer(new ProducerConfig(props));
        KeyedMessage msg = new KeyedMessage(testTopic, "test message".getBytes());
        p.send(msg);

        ZkHosts hosts = new ZkHosts(connectionString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, testTopic, "/", "testConsumerGroup");

        Map stormConf = new HashMap();

        Broker broker = new Broker("localhost", testBroker.getPort());
        Partition testPartition = new Partition(broker, 0);

        dataStore = new KafkaDataStore(stormConf, spoutConfig, testPartition);
    }

    @After
    public void shutdown() throws Exception {
        testBroker.shutdown();
    }

    @Test
    public void testStoreReadWrite() {
        Long offset = 100L;
        String testData = "abcdefg";

        dataStore.write(offset, testData);
        String readBack = dataStore.read();

        assertEquals(testData, readBack);
    }
}
