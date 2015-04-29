package storm.kafka;

import com.google.common.collect.Lists;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class KafkaApiBrokerReaderTest {

    public static final int DEFAULT_NUMBER_OF_PARTITIONS = 1;
    private KafkaTestBroker broker;
    private ArrayList<Broker> seedBrokers;
    private String testTopic;

    @Before
    public void setup() {
        broker = new KafkaTestBroker(DEFAULT_NUMBER_OF_PARTITIONS);
        Broker seed = Broker.fromString(broker.getBrokerConnectionString());
        seedBrokers = Lists.newArrayList(seed);
        testTopic = "testTopic";
        TestUtils.createTopicAndSendMessage(broker, testTopic, null, "someValue");
    }

    @After
    public void shutdown() {
        broker.shutdown();
    }

    @Test
    public void testGetCurrentBrokers() throws Exception {
        KafkaApiBrokerReader reader = new KafkaApiBrokerReader(seedBrokers, testTopic);
        GlobalPartitionInformation currentBrokers = reader.getCurrentBrokers();
        List<Partition> orderedPartitions = currentBrokers.getOrderedPartitions();
        assertEquals(1, orderedPartitions.size());
        for (Partition partition: orderedPartitions) {
            assertEquals(seedBrokers.get(0), partition.host);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidTopic() {
        new KafkaApiBrokerReader(seedBrokers, null).getCurrentBrokers();
    }


    @Test
    public void testGetCurrentBrokersSkipErrors() throws Exception {
        seedBrokers.add(null);
        Collections.reverse(seedBrokers);
        KafkaApiBrokerReader reader = new KafkaApiBrokerReader(seedBrokers, testTopic);
        GlobalPartitionInformation currentBrokers = reader.getCurrentBrokers();
        List<Partition> orderedPartitions = currentBrokers.getOrderedPartitions();
        assertEquals(1, orderedPartitions.size());
        for (Partition partition: orderedPartitions) {
            assertEquals(seedBrokers.get(1), partition.host);
        }
    }

    @Test
    public void testGetCurrentBrokersExitsEarly() throws Exception {
        int wrongNumberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS + 1;
        KafkaTestBroker brokerWithWrongNumberOfPartitions = new KafkaTestBroker(wrongNumberOfPartitions);
        TestUtils.createTopicAndSendMessage(brokerWithWrongNumberOfPartitions, testTopic, null, "someValue");
        seedBrokers.add(Broker.fromString(brokerWithWrongNumberOfPartitions.getBrokerConnectionString()));
        KafkaApiBrokerReader reader = new KafkaApiBrokerReader(seedBrokers, testTopic);
        GlobalPartitionInformation currentBrokers = reader.getCurrentBrokers();
        List<Partition> orderedPartitions = currentBrokers.getOrderedPartitions();
        assertEquals(DEFAULT_NUMBER_OF_PARTITIONS, orderedPartitions.size());
        assertEquals(1, orderedPartitions.size());
        for (Partition partition: orderedPartitions) {
            assertEquals(seedBrokers.get(0), partition.host);
        }
        brokerWithWrongNumberOfPartitions.shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void testGetCurrentBrokersWithInvalidTopic() throws Exception {
        new KafkaApiBrokerReader(seedBrokers, "noTopic").getCurrentBrokers();
    }

}
