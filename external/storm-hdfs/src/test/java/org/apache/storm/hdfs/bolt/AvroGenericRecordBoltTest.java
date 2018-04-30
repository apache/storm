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

package org.apache.storm.hdfs.bolt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.storm.Config;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.testing.MiniDFSClusterRule;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroGenericRecordBoltTest {

    private static final String testRoot = "/unittest";
    private static final String schemaV1 = "{\"type\":\"record\","
                                           + "\"name\":\"myrecord\","
                                           + "\"fields\":[{\"name\":\"foo1\",\"type\":\"string\"},"
                                           + "{ \"name\":\"int1\", \"type\":\"int\" }]}";
    private static final String schemaV2 = "{\"type\":\"record\","
                                           + "\"name\":\"myrecord\","
                                           + "\"fields\":[{\"name\":\"foo1\",\"type\":\"string\"},"
                                           + "{ \"name\":\"bar\", \"type\":\"string\", \"default\":\"baz\" },"
                                           + "{ \"name\":\"int1\", \"type\":\"int\" }]}";
    private static Schema schema1;
    private static Schema schema2;
    private static Tuple tuple1;
    private static Tuple tuple2;
    @Rule
    public MiniDFSClusterRule dfsClusterRule = new MiniDFSClusterRule(() -> {
        Configuration conf = new Configuration();
        conf.set("fs.trash.interval", "10");
        conf.setBoolean("dfs.permissions", true);
        File baseDir = new File("./target/hdfs/").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        return conf;
    });
    @Mock
    private OutputCollector collector;
    @Mock
    private TopologyContext topologyContext;
    private DistributedFileSystem fs;
    private String hdfsURI;

    @BeforeClass
    public static void setupClass() {
        Schema.Parser parser = new Schema.Parser();
        schema1 = parser.parse(schemaV1);

        parser = new Schema.Parser();
        schema2 = parser.parse(schemaV2);

        GenericRecordBuilder builder1 = new GenericRecordBuilder(schema1);
        builder1.set("foo1", "bar1");
        builder1.set("int1", 1);
        tuple1 = generateTestTuple(builder1.build());

        GenericRecordBuilder builder2 = new GenericRecordBuilder(schema2);
        builder2.set("foo1", "bar2");
        builder2.set("int1", 2);
        tuple2 = generateTestTuple(builder2.build());
    }

    private static Tuple generateTestTuple(GenericRecord record) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                                                                            new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("record");
            }
        };
        return new TupleImpl(topologyContext, new Values(record), topologyContext.getComponentId(1), 1, "");
    }

    @Before
    public void setup() throws Exception {
        fs = dfsClusterRule.getDfscluster().getFileSystem();
        hdfsURI = fs.getUri() + "/";
    }

    @After
    public void shutDown() throws IOException {
        fs.close();
    }

    @Test
    public void multipleTuplesOneFile() throws IOException {
        AvroGenericRecordBolt bolt = makeAvroBolt(hdfsURI, 1, 1f, schemaV1);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple1);
        bolt.execute(tuple1);
        bolt.execute(tuple1);

        Assert.assertEquals(1, countNonZeroLengthFiles(testRoot));
        verifyAllAvroFiles(testRoot);
    }

    @Test
    public void multipleTuplesMutliplesFiles() throws IOException {
        AvroGenericRecordBolt bolt = makeAvroBolt(hdfsURI, 1, .0001f, schemaV1);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple1);
        bolt.execute(tuple1);
        bolt.execute(tuple1);

        Assert.assertEquals(4, countNonZeroLengthFiles(testRoot));
        verifyAllAvroFiles(testRoot);
    }

    @Test
    public void forwardSchemaChangeWorks() throws IOException {
        AvroGenericRecordBolt bolt = makeAvroBolt(hdfsURI, 1, 1000f, schemaV1);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple2);

        //Schema change should have forced a rotation
        Assert.assertEquals(2, countNonZeroLengthFiles(testRoot));

        verifyAllAvroFiles(testRoot);
    }

    @Test
    public void backwardSchemaChangeWorks() throws IOException {
        AvroGenericRecordBolt bolt = makeAvroBolt(hdfsURI, 1, 1000f, schemaV2);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple2);

        //Schema changes should have forced file rotations
        Assert.assertEquals(2, countNonZeroLengthFiles(testRoot));
        verifyAllAvroFiles(testRoot);
    }

    @Test
    public void schemaThrashing() throws IOException {
        AvroGenericRecordBolt bolt = makeAvroBolt(hdfsURI, 1, 1000f, schemaV2);

        bolt.prepare(new Config(), topologyContext, collector);
        bolt.execute(tuple1);
        bolt.execute(tuple2);
        bolt.execute(tuple1);
        bolt.execute(tuple2);
        bolt.execute(tuple1);
        bolt.execute(tuple2);
        bolt.execute(tuple1);
        bolt.execute(tuple2);

        //Two distinct schema should result in only two files
        Assert.assertEquals(2, countNonZeroLengthFiles(testRoot));
        verifyAllAvroFiles(testRoot);
    }

    private AvroGenericRecordBolt makeAvroBolt(String nameNodeAddr, int countSync, float rotationSizeMB, String schemaAsString) {

        SyncPolicy fieldsSyncPolicy = new CountSyncPolicy(countSync);

        FileNameFormat fieldsFileNameFormat = new DefaultFileNameFormat().withPath(testRoot);

        FileRotationPolicy rotationPolicy =
            new FileSizeRotationPolicy(rotationSizeMB, FileSizeRotationPolicy.Units.MB);

        return new AvroGenericRecordBolt()
            .withFsUrl(nameNodeAddr)
            .withFileNameFormat(fieldsFileNameFormat)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(fieldsSyncPolicy);
    }

    private void verifyAllAvroFiles(String path) throws IOException {
        Path p = new Path(path);

        for (FileStatus file : fs.listStatus(p)) {
            if (file.getLen() > 0) {
                fileIsGoodAvro(file.getPath());
            }
        }
    }

    private int countNonZeroLengthFiles(String path) throws IOException {
        Path p = new Path(path);
        int nonZero = 0;

        for (FileStatus file : fs.listStatus(p)) {
            if (file.getLen() > 0) {
                nonZero++;
            }
        }

        return nonZero;
    }

    private void fileIsGoodAvro(Path path) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (FSDataInputStream in = fs.open(path, 0); FileOutputStream out = new FileOutputStream("target/FOO.avro")) {
            byte[] buffer = new byte[100];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
        }

        java.io.File file = new File("target/FOO.avro");

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
            }
        }

        file.delete();
    }
}
