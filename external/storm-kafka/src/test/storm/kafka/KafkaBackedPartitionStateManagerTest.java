package storm.kafka;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class KafkaBackedPartitionStateManagerTest {

    @Mock
    private KafkaDataStore dataStore;

    private String stateJson = "{\n" +
                    "    \"broker\": {\n" +
                    "        \"host\": \"kafka.sample.net\",\n" +
                    "        \"port\": 9092\n" +
                    "    },\n" +
                    "    \"offset\": 4285,\n" +
                    "    \"partition\": 1,\n" +
                    "    \"topic\": \"testTopic\",\n" +
                    "    \"topology\": {\n" +
                    "        \"id\": \"fce905ff-25e0 -409e-bc3a-d855f 787d13b\",\n" +
                    "        \"name\": \"Test Topology\"\n" +
                    "    }\n" +
                    "}";

    private KafkaBackedPartitionStateManager partitionStateManager;



    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        Map stormConf = new HashMap();

        ZkHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "topic", "/test", "id");

        Broker broker = new Broker("localhost", 9100);
        Partition testPartition = new Partition(broker, 1);

        partitionStateManager = new KafkaBackedPartitionStateManager(stormConf, spoutConfig, testPartition, dataStore);
    }

    @Test
    public void testReadState() throws Exception {

        when(dataStore.read()).thenReturn(stateJson);

        Map state =  partitionStateManager.getState();

        assertEquals("kafka.sample.net", ((Map)state.get("broker")).get("host"));
        assertEquals(9092L, ((Map)state.get("broker")).get("port"));
        assertEquals(4285L, state.get("offset"));
        assertEquals(1L, state.get("partition"));
        assertEquals(4285L, state.get("offset"));
        assertEquals("testTopic", state.get("topic"));
        assertEquals("fce905ff-25e0 -409e-bc3a-d855f 787d13b", ((Map)state.get("topology")).get("id"));
        assertEquals("Test Topology", ((Map)state.get("topology")).get("name"));
    }

    @Test
    public void testWriteState() throws Exception {

        Map broker = ImmutableMap.of("host", "kafka.sample.net", "port", 9092L);
        Map topology = ImmutableMap.of("id", "fce905ff-25e0 -409e-bc3a-d855f 787d13b", "name", "Test Topology");
        Map state = ImmutableMap.of("broker", broker, "offset", 4285L, "partition", 1L, "topic", "testTopic", "topology", topology);

        partitionStateManager.writeState(state);

        verify(dataStore).write(eq(4285L), anyString());
    }
}
