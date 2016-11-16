package org.apache.storm.cassandra.trident;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.cassandra.client.CassandraConf;
import org.apache.storm.cassandra.trident.state.MapStateFactoryBuilder;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class MapStateTest {

    private static Logger logger = LoggerFactory.getLogger(MapStateTest.class);
    private static Config stormConf;
    private static Cluster cluster;
    private Session session;

    @Test
    public void nonTransactionalStateTest() throws Exception {

        StateFactory factory = MapStateFactoryBuilder.nontransactional()
                .withTable("words_ks", "words_table")
                .withKeys("word")
                .withJSONBinaryState("state")
                .build();

        wordsTest(factory);
    }

    @Test
    public void transactionalStateTest() throws Exception {

        StateFactory factory = MapStateFactoryBuilder.transactional()
                .withTable("words_ks", "words_table")
                .withKeys("word")
                .withJSONBinaryState("state")
                .build();

        wordsTest(factory);
    }

    @Test
    public void opaqueStateTest() throws Exception {

        StateFactory factory = MapStateFactoryBuilder.opaque()
                .withTable("words_ks", "words_table")
                .withKeys("word")
                .withJSONBinaryState("state")
                .build();

        wordsTest(factory);
    }

    public void wordsTest(StateFactory factory) throws Exception {

        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();

        TridentState wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                .persistentAggregate(factory, new Count(), new Fields("state")).parallelismHint(1);

        LocalDRPC client = new LocalDRPC();
        topology.newDRPCStream("words", client)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("state"))
                .each(new Fields("state"), new FilterNull())
                .aggregate(new Fields("state"), new Sum(), new Fields("sum"));

        LocalCluster cluster = new LocalCluster();
        logger.info("Submitting topology.");
        cluster.submitTopology("test", stormConf, topology.build());

        logger.info("Waiting for something to happen.");
        int count;
        do {
            Thread.sleep(2000);
            count = session.execute(QueryBuilder.select().all().from("words_ks", "words_table"))
                    .getAvailableWithoutFetching();
            logger.info("Found {} records", count);
        } while (count < 24);

        logger.info("Starting queries.");
        assertEquals("[[5]]", client.execute("words", "cat dog the man")); // 5
        assertEquals("[[0]]", client.execute("words", "cat")); // 0
        assertEquals("[[0]]", client.execute("words", "dog")); // 0
        assertEquals("[[4]]", client.execute("words", "the")); // 4
        assertEquals("[[1]]", client.execute("words", "man")); // 1

        cluster.shutdown();

    }

    @BeforeClass
    public static void setUpClass() throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        stormConf = new Config();
        stormConf.put(CassandraConf.CASSANDRA_NODES, EmbeddedCassandraServerHelper.getHost());
        stormConf.put(CassandraConf.CASSANDRA_PORT, EmbeddedCassandraServerHelper.getNativeTransportPort());
        stormConf.put(CassandraConf.CASSANDRA_KEYSPACE, "words_ks");

        Cluster.Builder clusterBuilder = Cluster.builder();

        // Add cassandra cluster contact points
        clusterBuilder.addContactPoint(EmbeddedCassandraServerHelper.getHost());
        clusterBuilder.withPort(EmbeddedCassandraServerHelper.getNativeTransportPort());

        // Build cluster and connect
        cluster = clusterBuilder.build();
    }

    @Before
    public void setUp() throws Exception {

        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        session = cluster.connect();

        createKeyspace("words_ks");
        createTable("words_ks", "words_table",
                column("word", DataType.varchar()),
                column("state", DataType.blob()));

    }

    @After
    public void tearDown() {
        session.close();
    }

    protected void createKeyspace(String keyspace) throws Exception {
        // Create keyspace not supported in the current datastax driver
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS "
                + keyspace
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        logger.info(createKeyspace);
        if (!session.execute(createKeyspace)
            .wasApplied()) {
            throw new Exception("Did not create keyspace " + keyspace);
        }
    }

    protected void createTable(String keyspace, String table, Column key, Column... fields) {
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", SimpleStrategy.class.getSimpleName());
        replication.put("replication_factor", 1);

        Create createTable = SchemaBuilder.createTable(keyspace, table)
                .addPartitionKey(key.name, key.type);
        for (Column field : fields) {
            createTable.addColumn(field.name, field.type);
        }
        logger.info(createTable.toString());
        session.execute(createTable);
    }

    protected static Column column(String name, DataType type) {
        Column column = new Column();
        column.name = name;
        column.type = type;
        return column;
    }

    protected static class Column {
        public String name;
        public DataType type;
    }

}
