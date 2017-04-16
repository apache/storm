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

import com.google.common.base.Preconditions;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.format.SimpleFileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.TimedRotationPolicy;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.SimpleSqlTridentConsumer;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Create a HDFS sink based on the URI and properties. The URI has the format of hdfs://host:port/path-to-file
 * The properties are in JSON format which specifies the name / path of the hdfs file and etc.
 */
public class HdfsDataSourcesProvider implements DataSourcesProvider {

  private static class HdfsTridentDataSource implements ISqlTridentDataSource {
    private final String url;
    private final Properties props;
    private final IOutputSerializer serializer;

    private HdfsTridentDataSource(String url, Properties props, IOutputSerializer serializer) {
      this.url = url;
      this.props = props;
      this.serializer = serializer;
    }

    @Override
    public ITridentDataSource getProducer() {
      throw new UnsupportedOperationException(this.getClass().getName() + " doesn't provide Producer");
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      FileNameFormat fileNameFormat = new SimpleFileNameFormat()
          .withPath(props.getProperty("hdfs.file.path", "/storm"))
          .withName(props.getProperty("hdfs.file.name", "$TIME.$NUM.txt"));

      RecordFormat recordFormat = new TridentRecordFormat(serializer);

      FileRotationPolicy rotationPolicy;
      String size = props.getProperty("hdfs.rotation.size.kb");
      String interval = props.getProperty("hdfs.rotation.time.seconds");
      Preconditions.checkArgument(size != null || interval != null, "Hdfs data source must contain file rotation config");

      if (size != null) {
        rotationPolicy = new FileSizeRotationPolicy(Float.parseFloat(size), FileSizeRotationPolicy.Units.KB);
      } else {
        rotationPolicy = new TimedRotationPolicy(Float.parseFloat(interval), TimedRotationPolicy.TimeUnit.SECONDS);
      }

      HdfsState.Options options = new HdfsState.HdfsFileOptions()
          .withFileNameFormat(fileNameFormat)
          .withRecordFormat(recordFormat)
          .withRotationPolicy(rotationPolicy)
          .withFsUrl(url);

      StateFactory stateFactory = new HdfsStateFactory().withOptions(options);
      StateUpdater stateUpdater = new HdfsUpdater();

      return new SimpleSqlTridentConsumer(stateFactory, stateUpdater);
    }
  }

  private static class TridentRecordFormat implements RecordFormat {
    private final IOutputSerializer serializer;

    private TridentRecordFormat(IOutputSerializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public byte[] format(TridentTuple tuple) {
      //TODO we should handle '\n'. ref DelimitedRecordFormat
      return serializer.write(tuple.getValues(), null).array();
    }

  }

  @Override
  public String scheme() {
    return "hdfs";
  }

  @Override
  public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass,
                              List<FieldInfo> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                Properties properties, List<FieldInfo> fields) {
    List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
    IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);
    return new HdfsTridentDataSource(uri.toString(), properties, serializer);
  }

}
