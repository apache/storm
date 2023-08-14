/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.trident;

import java.util.HashMap;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.cassandra.client.CassandraConf;
import org.apache.storm.cassandra.testtools.EmbeddedCassandraResource;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapStateTest {

    @RegisterExtension
    public static final EmbeddedCassandraResource cassandra= new EmbeddedCassandraResource(20000);

    private static final Logger logger = LoggerFactory.getLogger(MapStateTest.class);
    private CqlSession cqlSession;

    protected static Column column(String name, DataType type) {
        Column column = new Column();
        column.name = name;
        column.type = type;
        return column;
    }

    @Test
    public void nonTransactionalStateTest() throws Exception {
        StateFactory factory = MapStateFactoryBuilder.nontransactional(getCassandraConfig())
                                                     .withTable("words_ks", "words_table")
                                                     .withKeys("word")
                                                     .withJSONBinaryState("state")
                                                     .build();

        wordsTest(factory);
    }

    @Test
    public void transactionalStateTest() throws Exception {
        StateFactory factory = MapStateFactoryBuilder.transactional(getCassandraConfig())
                                                     .withTable("words_ks", "words_table")
                                                     .withKeys("word")
                                                     .withJSONBinaryState("state")
                                                     .build();

        wordsTest(factory);
    }

    @Test
    public void opaqueStateTest() throws Exception {
        StateFactory factory = MapStateFactoryBuilder.opaque(getCassandraConfig())
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
                                          .each(new Fields("sentence"), new Split(), new Fields("word"))
                                          .groupBy(new Fields("word"))
                                          .persistentAggregate(factory, new Count(), new Fields("state"))
                                          .parallelismHint(1);

        LocalCluster cluster = new LocalCluster();
        LocalDRPC client = new LocalDRPC(cluster.getMetricRegistry());
        
        topology.newDRPCStream("words", client)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("state"))
                .each(new Fields("state"), new FilterNull())
                .aggregate(new Fields("state"), new Sum(), new Fields("sum"));
        
        logger.info("Submitting topology.");
        cluster.submitTopology("test", new HashMap<>(), topology.build());

        logger.info("Waiting for something to happen.");
        int count;
        do {
            Thread.sleep(2000);
            count = cqlSession.execute(QueryBuilder.selectFrom("words_ks", "words_table").all().asCql())
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

    @BeforeEach
    public void setUp() throws Exception {
        cqlSession = cassandra.getSession();
        createKeyspace("words_ks");
        createTable("words_ks", "words_table",
                    column("word", DataTypes.TEXT),
                    column("state", DataTypes.BLOB));
    }

    @AfterEach
    public void tearDown() {
        truncateTable("words_ks", "words_table");
    }

    protected void createKeyspace(String keyspace) throws Exception {
        // Create keyspace not supported in the current datastax driver
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS "
                                + keyspace
                                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        logger.info(createKeyspace);
        if (!cqlSession.execute(createKeyspace)
                    .wasApplied()) {
            throw new Exception("Did not create keyspace " + keyspace);
        }
    }

    protected Config getCassandraConfig() {
        Config cassandraConf = new Config();
        cassandraConf.put(CassandraConf.CASSANDRA_NODES, cassandra.getHost());
        cassandraConf.put(CassandraConf.CASSANDRA_PORT, cassandra.getNativeTransportPort());
        cassandraConf.put(CassandraConf.CASSANDRA_KEYSPACE, "words_ks");
        return cassandraConf;
    }

    protected void truncateTable(String keyspace, String table) {
        Truncate truncate = QueryBuilder.truncate(keyspace, table);
        cqlSession.execute(truncate.asCql());
    }

    protected void createTable(String keyspace, String table, Column key, Column... fields) {
        CreateTable createTable = SchemaBuilder.createTable(keyspace, table)
                                          .ifNotExists()
                                          .withPartitionKey(key.name, key.type);
        for (Column field : fields) {
            createTable.withColumn(field.name, field.type);
        }
        logger.info(createTable.toString());
        cqlSession.execute(createTable.asCql());
    }

    protected static class Column {
        public String name;
        public DataType type;
    }

}
