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

import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.format.SimpleFileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Create a HDFS sink based on the URI and properties. The URI has the format of hdfs://host:port/path-to-file
 * The properties are in JSON format which specifies the name / path of the hdfs file and etc.
 */
public class HdfsDataSourcesProvider implements DataSourcesProvider {

    private static final String PROPERTY_HDFS_FILE_PATH = "hdfs.file.path";
    private static final String PROPERTY_HDFS_FILE_NAME = "hdfs.file.name";
    private static final String DEFAULT_VALUE_HDFS_FILE_PATH = "/storm";
    private static final String DEFAULT_VALUE_HDF_FILE_NAME = "$TIME.$NUM.txt";
    private static final String PROPERTY_HDFS_ROTATION_SIZE_KB = "hdfs.rotation.size.kb";
    private static final String PROPERTY_HDFS_ROTATION_TIME_SECONDS = "hdfs.rotation.time.seconds";
    private static final String SCHEME_NAME = "hdfs";

    private static class HdfsStreamsDataSource implements ISqlStreamsDataSource {
        private final String url;
        private final Properties props;
        private final IOutputSerializer serializer;

        private HdfsStreamsDataSource(String url, Properties props, IOutputSerializer serializer) {
            this.url = url;
            this.props = props;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            throw new UnsupportedOperationException(this.getClass().getName() + " doesn't provide Producer");
        }

        @Override
        public IRichBolt getConsumer() {
            FileNameFormat fileNameFormat = new SimpleFileNameFormat()
                    .withPath(props.getProperty(PROPERTY_HDFS_FILE_PATH, DEFAULT_VALUE_HDFS_FILE_PATH))
                    .withName(props.getProperty(PROPERTY_HDFS_FILE_NAME, DEFAULT_VALUE_HDF_FILE_NAME));

            RecordFormat recordFormat = new StreamsRecordFormat(serializer);

            FileRotationPolicy rotationPolicy;
            String size = props.getProperty(PROPERTY_HDFS_ROTATION_SIZE_KB);
            String interval = props.getProperty(PROPERTY_HDFS_ROTATION_TIME_SECONDS);
            Preconditions.checkArgument(size != null || interval != null, "Hdfs data source must contain file rotation config");

            if (size != null) {
                rotationPolicy = new FileSizeRotationPolicy(Float.parseFloat(size), FileSizeRotationPolicy.Units.KB);
            } else {
                rotationPolicy = new TimedRotationPolicy(Float.parseFloat(interval), TimedRotationPolicy.TimeUnit.SECONDS);
            }

            return new HdfsBolt()
                    .withFileNameFormat(fileNameFormat)
                    .withRecordFormat(recordFormat)
                    .withRotationPolicy(rotationPolicy)
                    .withFsUrl(url);
        }
    }

    private static class StreamsRecordFormat implements RecordFormat {
        private final IOutputSerializer serializer;

        private StreamsRecordFormat(IOutputSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        public byte[] format(Tuple tuple) {
            //TODO we should handle '\n'. ref DelimitedRecordFormat
            Values values = (Values) tuple.getValue(1);
            return serializer.write(values, null).array();
        }

    }

    @Override
    public String scheme() {
        return SCHEME_NAME;
    }

    @Override
    public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
        List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);
        return new HdfsStreamsDataSource(uri.toString(), properties, serializer);
    }

}
