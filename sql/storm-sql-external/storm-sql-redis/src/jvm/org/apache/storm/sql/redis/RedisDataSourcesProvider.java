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

package org.apache.storm.sql.redis;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import redis.clients.util.JedisURIHelper;

/**
 * Create a Redis sink based on the URI and properties. The URI has the format of
 * redis://:[password]@[host]:[port]/[dbIdx]. Only host is mandatory and others can be set to default.
 * <p/>
 * The properties are in JSON format which specifies the config of the Redis data type and etc.
 * Please note that when "use.redis.cluster" is "true", cluster discovery is only done from given URI.
 */
public class RedisDataSourcesProvider implements DataSourcesProvider {
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final int DEFAULT_TIMEOUT = 2000;
    private static final String PROPERTY_DATA_TYPE = "data.type";
    private static final String PROPERTY_DATA_ADDITIONAL_KEY = "data.additional.key";
    private static final String PROPERTY_REDIS_TIMEOUT = "redis.timeout";
    private static final String PROPERTY_USE_REDIS_CLUSTER = "use.redis.cluster";
    private static final String DEFAULT_USE_REDIS_CLUSTER = "false";

    private abstract static class AbstractRedisStreamsDataSource implements ISqlStreamsDataSource, Serializable {
        protected abstract IRichBolt newRedisBolt(RedisStoreMapper storeMapper);

        private final Properties props;
        private final List<FieldInfo> fields;
        private final IOutputSerializer serializer;

        AbstractRedisStreamsDataSource(Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
            this.props = props;
            this.fields = fields;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            throw new UnsupportedOperationException(this.getClass().getName() + " doesn't provide Producer");
        }

        @Override
        public IRichBolt getConsumer() {
            RedisDataTypeDescription dataTypeDescription = getDataTypeDesc(props);

            RedisStoreMapper storeMapper = new SqlRedisStoreMapper(dataTypeDescription, fields, serializer);
            return newRedisBolt(storeMapper);
        }

        private RedisDataTypeDescription getDataTypeDesc(Properties props) {
            Preconditions.checkArgument(props.containsKey(PROPERTY_DATA_TYPE),
                    "Redis data source must contain " + PROPERTY_DATA_TYPE + " config");

            RedisDataTypeDescription.RedisDataType dataType = RedisDataTypeDescription.RedisDataType.valueOf(
                    props.getProperty(PROPERTY_DATA_TYPE).toUpperCase());
            String additionalKey = props.getProperty(PROPERTY_DATA_ADDITIONAL_KEY);

            return new RedisDataTypeDescription(dataType, additionalKey);
        }
    }

    private static class RedisClusterStreamsDataSource extends AbstractRedisStreamsDataSource {
        private final JedisClusterConfig config;

        RedisClusterStreamsDataSource(JedisClusterConfig config, Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
            super(props, fields, serializer);
            this.config = config;
        }

        @Override
        protected IRichBolt newRedisBolt(RedisStoreMapper storeMapper) {
            return new RedisStoreBolt(config, storeMapper);
        }
    }

    private static class RedisStreamsDataSource extends AbstractRedisStreamsDataSource {
        private final JedisPoolConfig config;

        RedisStreamsDataSource(JedisPoolConfig config, Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
            super(props, fields, serializer);
            this.config = config;
        }

        @Override
        protected IRichBolt newRedisBolt(RedisStoreMapper storeMapper) {
            return new RedisStoreBolt(config, storeMapper);
        }
    }

    @Override
    public String scheme() {
        return "redis";
    }

    @Override
    public ISqlStreamsDataSource constructStreams(
            URI uri, String inputFormatClass, String outputFormatClass, Properties props, List<FieldInfo> fields) {
        Preconditions.checkArgument(JedisURIHelper.isValid(uri), "URI is not valid for Redis: " + uri);

        String host = uri.getHost();
        int port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_REDIS_PORT;
        int dbIdx = JedisURIHelper.getDBIndex(uri);
        String password = JedisURIHelper.getPassword(uri);

        int timeout = Integer.parseInt(props.getProperty(PROPERTY_REDIS_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT)));

        boolean clusterMode = Boolean.valueOf(props.getProperty(PROPERTY_USE_REDIS_CLUSTER, "false"));

        List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, props, fieldNames);
        if (clusterMode) {
            JedisClusterConfig config = new JedisClusterConfig.Builder()
                    .setNodes(Collections.singleton(new InetSocketAddress(host, port)))
                    .setTimeout(timeout)
                    .build();
            return new RedisClusterStreamsDataSource(config, props, fields, serializer);
        } else {
            JedisPoolConfig config = new JedisPoolConfig(host, port, timeout, password, dbIdx);
            return new RedisStreamsDataSource(config, props, fields, serializer);
        }
    }

    private static class SqlRedisStoreMapper implements RedisStoreMapper {
        private final RedisDataTypeDescription dataTypeDescription;
        private final FieldInfo primaryKeyField;
        private final IOutputSerializer outputSerializer;

        private SqlRedisStoreMapper(RedisDataTypeDescription dataTypeDescription, List<FieldInfo> fields,
                                    IOutputSerializer outputSerializer) {
            this.dataTypeDescription = dataTypeDescription;
            this.outputSerializer = outputSerializer;

            // find primary key from constructor
            FieldInfo pkField = findPrimaryKeyField(fields);
            Preconditions.checkArgument(pkField != null, "Primary key must be presented to field list");

            this.primaryKeyField = pkField;
        }

        private FieldInfo findPrimaryKeyField(List<FieldInfo> fields) {
            FieldInfo pkField = null;
            for (FieldInfo field : fields) {
                if (field.isPrimary()) {
                    // TODO: this assumes key is only from the one field
                    // if not we need to have order of fields in PK
                    pkField = field;
                    break;
                }
            }
            return pkField;
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return dataTypeDescription;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            Object key = tuple.getValue(0);
            if (key == null) {
                throw new NullPointerException("key field is null");
            }
            return String.valueOf(key);
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            Values values = (Values) tuple.getValue(1);
            byte[] array = outputSerializer.write(values, null).array();
            return new String(array);
        }
    }
}
