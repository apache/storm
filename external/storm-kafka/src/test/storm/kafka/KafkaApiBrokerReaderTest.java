package storm.kafka;

import com.google.common.collect.Lists;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.List;

import static org.junit.Assert.*;

public class KafkaApiBrokerReaderTest {

    private KafkaTestBroker broker;
    private Broker seed;

    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        seed = Broker.fromString(broker.getBrokerConnectionString());
    }

    @After
    public void shutdown() {
        broker.shutdown();
    }

    @Test
    public void testGetCurrentBrokers() throws Exception {
        String testTopic = "testTopic";
        TestUtils.createTopicAndSendMessage(broker, testTopic, null, "someValue");
        KafkaApiBrokerReader reader = new KafkaApiBrokerReader(Lists.newArrayList(seed), testTopic);
        GlobalPartitionInformation currentBrokers = reader.getCurrentBrokers();
        List<Partition> orderedPartitions = currentBrokers.getOrderedPartitions();
        for (Partition partition: orderedPartitions) {
            assertEquals(seed, partition.host);
        }
    }

    @Test
    public void testGetCurrentBrokersWithInvalidTopic() throws Exception {
        KafkaApiBrokerReader reader = new KafkaApiBrokerReader(Lists.newArrayList(seed), "noTopic");
        GlobalPartitionInformation currentBrokers = reader.getCurrentBrokers();
        List<Partition> orderedPartitions = currentBrokers.getOrderedPartitions();
        assertTrue(orderedPartitions.isEmpty());
    }

}
