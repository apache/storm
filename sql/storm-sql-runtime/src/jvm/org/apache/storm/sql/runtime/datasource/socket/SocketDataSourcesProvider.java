/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.runtime.datasource.socket;

import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.storm.spout.Scheme;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.datasource.socket.bolt.SocketBolt;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;

/**
 * Create a Socket data source based on the URI and properties. The URI has the format of
 * socket://[host]:[port]. Both of host and port are mandatory.
 * <p/>
 * Note that it connects to given host and port, and receive the message if it's used for input source,
 * and send the message if it's used for output data source.
 */
public class SocketDataSourcesProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
        return "socket";
    }

    private static class SocketStreamsDataSource implements ISqlStreamsDataSource {

        private final String host;
        private final int port;
        private final Scheme scheme;
        private final IOutputSerializer serializer;

        SocketStreamsDataSource(String host, int port, Scheme scheme, IOutputSerializer serializer) {
            this.host = host;
            this.port = port;
            this.scheme = scheme;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            return new SocketSpout(scheme, host, port);
        }

        @Override
        public IRichBolt getConsumer() {
            return new SocketBolt(serializer, host, port);
        }
    }

    @Override
    public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            throw new RuntimeException("Port information is not available. URI: " + uri);
        }

        List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
        Scheme scheme = SerdeUtils.getScheme(inputFormatClass, properties, fieldNames);
        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);

        return new SocketDataSourcesProvider.SocketStreamsDataSource(host, port, scheme, serializer);
    }
}
