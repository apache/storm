/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hive.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.storm.Config;
import org.apache.storm.hive.bolt.HiveSetupUtil;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TestHiveWriter {
    public static final String PART1_NAME = "city";
    public static final String PART2_NAME = "state";
    public static final String[] partNames = { PART1_NAME, PART2_NAME };
    final static String dbName = "testdb";
    final static String tblName = "test_table2";
    final String[] partitionVals = { "sunnyvale", "ca" };
    final String[] colNames = { "id", "msg" };
    private final int port;
    private final String metaStoreURI;
    private final HiveConf conf;
    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();
    int timeout = 10000; // msec
    UserGroupInformation ugi = null;
    private ExecutorService callTimeoutPool;

    public TestHiveWriter() throws Exception {
        port = 9083;
        metaStoreURI = null;
        int callTimeoutPoolSize = 1;
        callTimeoutPool = Executors.newFixedThreadPool(callTimeoutPoolSize,
                                                       new ThreadFactoryBuilder().setNameFormat("hiveWriterTest").build());

        // 1) Start metastore
        conf = HiveSetupUtil.getHiveConf();
        TxnDbUtil.setConfValues(conf);
        if (metaStoreURI != null) {
            conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);
        }
    }

    @Test
    public void testInstantiate() throws Exception {
        DelimitedRecordHiveMapper mapper = new MockedDelemiteredRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        TestingHiveWriter writer = new TestingHiveWriter(endPoint, 10, true, timeout
            , callTimeoutPool, mapper, ugi, false);
        writer.close();
    }

    @Test
    public void testWriteBasic() throws Exception {
        DelimitedRecordHiveMapper mapper = new MockedDelemiteredRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        TestingHiveWriter writer = new TestingHiveWriter(endPoint, 10, true, timeout
            , callTimeoutPool, mapper, ugi, false);
        writeTuples(writer, mapper, 3);
        writer.flush(false);
        writer.close();
        Mockito.verify(writer.getMockedTxBatch(), Mockito.times(3)).write(Mockito.any(byte[].class));
    }

    @Test
    public void testWriteMultiFlush() throws Exception {
        DelimitedRecordHiveMapper mapper = new MockedDelemiteredRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));

        HiveEndPoint endPoint = new HiveEndPoint(metaStoreURI, dbName, tblName, Arrays.asList(partitionVals));
        TestingHiveWriter writer = new TestingHiveWriter(endPoint, 10, true, timeout
            , callTimeoutPool, mapper, ugi, false);
        Tuple tuple = generateTestTuple("1", "abc");
        writer.write(mapper.mapRecord(tuple));
        tuple = generateTestTuple("2", "def");
        writer.write(mapper.mapRecord(tuple));
        Assert.assertEquals(writer.getTotalRecords(), 2);
        Mockito.verify(writer.getMockedTxBatch(), Mockito.times(2)).write(Mockito.any(byte[].class));
        Mockito.verify(writer.getMockedTxBatch(), Mockito.never()).commit();
        writer.flush(true);
        Assert.assertEquals(writer.getTotalRecords(), 0);
        Mockito.verify(writer.getMockedTxBatch(), Mockito.atLeastOnce()).commit();

        tuple = generateTestTuple("3", "ghi");
        writer.write(mapper.mapRecord(tuple));
        writer.flush(true);

        tuple = generateTestTuple("4", "klm");
        writer.write(mapper.mapRecord(tuple));
        writer.flush(true);
        writer.close();
        Mockito.verify(writer.getMockedTxBatch(), Mockito.times(4)).write(Mockito.any(byte[].class));
    }

    private Tuple generateTestTuple(Object id, Object msg) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                                                                            new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("id", "msg");
            }
        };
        return new TupleImpl(topologyContext, new Values(id, msg), "", 1, "");
    }

    private void writeTuples(HiveWriter writer, HiveMapper mapper, int count)
        throws HiveWriter.WriteFailure, InterruptedException, SerializationError {
        Integer id = 100;
        String msg = "test-123";
        for (int i = 1; i <= count; i++) {
            Tuple tuple = generateTestTuple(id, msg);
            writer.write(mapper.mapRecord(tuple));
        }
    }

    private static class TestingHiveWriter extends HiveWriter {

        private StreamingConnection mockedStreamingConn;
        private TransactionBatch mockedTxBatch;

        public TestingHiveWriter(HiveEndPoint endPoint, int txnsPerBatch, boolean autoCreatePartitions, long callTimeout,
                                 ExecutorService callTimeoutPool, HiveMapper mapper, UserGroupInformation ugi,
                                 boolean tokenAuthEnabled) throws InterruptedException, ConnectFailure {
            super(endPoint, txnsPerBatch, autoCreatePartitions, callTimeout, callTimeoutPool, mapper, ugi, tokenAuthEnabled);
        }

        @Override
        synchronized StreamingConnection newConnection(UserGroupInformation ugi, boolean tokenAuthEnabled) throws InterruptedException,
            ConnectFailure {
            if (mockedStreamingConn == null) {
                mockedStreamingConn = Mockito.mock(StreamingConnection.class);
                mockedTxBatch = Mockito.mock(TransactionBatch.class);

                try {
                    Mockito.when(mockedStreamingConn.fetchTransactionBatch(Mockito.anyInt(), Mockito.any(RecordWriter.class)))
                           .thenReturn(mockedTxBatch);
                } catch (StreamingException e) {
                    throw new RuntimeException(e);
                }
            }

            return mockedStreamingConn;
        }

        public TransactionBatch getMockedTxBatch() {
            return mockedTxBatch;
        }
    }

    private static class MockedDelemiteredRecordHiveMapper extends DelimitedRecordHiveMapper {
        private final RecordWriter mockedRecordWriter;

        public MockedDelemiteredRecordHiveMapper() {
            this.mockedRecordWriter = Mockito.mock(RecordWriter.class);
        }

        @Override
        public RecordWriter createRecordWriter(HiveEndPoint endPoint) throws StreamingException, IOException, ClassNotFoundException {
            return mockedRecordWriter;
        }

        public RecordWriter getMockedRecordWriter() {
            return mockedRecordWriter;
        }
    }

}