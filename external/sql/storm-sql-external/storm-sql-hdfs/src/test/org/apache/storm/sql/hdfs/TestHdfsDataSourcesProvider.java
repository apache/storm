/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.storm.hdfs.trident.HdfsState.HdfsFileOptions;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestHdfsDataSourcesProvider {
  private static final List<FieldInfo> FIELDS = ImmutableList.of(
      new FieldInfo("ID", int.class, true),
      new FieldInfo("val", String.class, false));
  private static final Properties TBL_PROPERTIES = new Properties();

  private static String hdfsURI;
  private static MiniDFSCluster hdfsCluster;

  static {
    TBL_PROPERTIES.put("hdfs.file.path", "/unittest");
    TBL_PROPERTIES.put("hdfs.file.name", "test1.txt");
    TBL_PROPERTIES.put("hdfs.rotation.time.seconds", "120");
  }

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.trash.interval", "10");
    conf.setBoolean("dfs.permissions", true);
    File baseDir = new File("./target/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    hdfsCluster = builder.build();
    hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
  }

  @After
  public void shutDown() throws IOException {
    hdfsCluster.shutdown();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHdfsSink() {
    ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
            URI.create(hdfsURI), null, null, TBL_PROPERTIES, FIELDS);
    Assert.assertNotNull(ds);

    ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

    Assert.assertEquals(HdfsStateFactory.class, consumer.getStateFactory().getClass());
    Assert.assertEquals(HdfsUpdater.class, consumer.getStateUpdater().getClass());

    HdfsState state = (HdfsState) consumer.getStateFactory().makeState(Collections.emptyMap(), null, 0, 1);
    StateUpdater stateUpdater = consumer.getStateUpdater();

    HdfsFileOptions options = mock(HdfsFileOptions.class);
    Whitebox.setInternalState(state, "options", options);

    List<TridentTuple> tupleList = mockTupleList();

    for (TridentTuple t : tupleList) {
      stateUpdater.updateState(state, Collections.singletonList(t), null);
      try {
        verify(options).execute(Collections.singletonList(t));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static List<TridentTuple> mockTupleList() {
    List<TridentTuple> tupleList = new ArrayList<>();
    TridentTuple t0 = mock(TridentTuple.class);
    TridentTuple t1 = mock(TridentTuple.class);
    doReturn(1).when(t0).get(0);
    doReturn(2).when(t1).get(0);
    doReturn(Lists.<Object>newArrayList(1, "2")).when(t0).getValues();
    doReturn(Lists.<Object>newArrayList(2, "3")).when(t1).getValues();
    tupleList.add(t0);
    tupleList.add(t1);
    return tupleList;
  }

}
