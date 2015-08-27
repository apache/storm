package storm.kafka;

import backtype.storm.Config;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ZKDataSourceTest {

    private TestingServer server;
    private ZkDataStore dataStore;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        server = new TestingServer();
        String connectionString = server.getConnectString();
        ZkHosts hosts = new ZkHosts(connectionString);

        SpoutConfig spoutConfig;
        spoutConfig = new SpoutConfig(hosts, "topic", "/test", "id");
        spoutConfig.zkServers = Arrays.asList("localhost");
        spoutConfig.zkPort = server.getPort();

        Map stormConf = new HashMap();
        stormConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, spoutConfig.zkPort);
        stormConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, spoutConfig.zkServers);
        stormConf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 20000);
        stormConf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 20000);
        stormConf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 3);
        stormConf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 30);

        Map storeConfig = new HashMap(stormConf);
        storeConfig.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, spoutConfig.zkServers);
        storeConfig.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, spoutConfig.zkPort);
        storeConfig.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, spoutConfig.zkRoot);

        dataStore = new ZkDataStore(storeConfig);
    }

    @After
    public void shutdown() throws Exception {
        dataStore.close();
        server.close();
    }

    @Test
    public void testStoreReadWrite() {

        String dataPath = "/myplace";
        String testData = "abcdefg";

        dataStore.write(dataPath, testData.getBytes());
        String readBack = new String(dataStore.read(dataPath));

        assertEquals(testData, readBack);
    }
}
