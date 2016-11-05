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
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisClusterState;
import org.apache.storm.redis.trident.state.RedisClusterStateUpdater;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.SimpleSqlTridentConsumer;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.tuple.ITuple;
import redis.clients.util.JedisURIHelper;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Create a Redis sink based on the URI and properties. The URI has the format of
 * redis://:[password]@[host]:[port]/[dbIdx]. Only host is mandatory and others can be set to default.
 *
 * The properties are in JSON format which specifies the config of the Redis data type and etc.
 * Please note that when "use.redis.cluster" is "true", cluster discovery is only done from given URI.
 */
public class RedisDataSourcesProvider implements DataSourcesProvider {
  private static final int DEFAULT_REDIS_PORT = 6379;
  private static final int DEFAULT_TIMEOUT = 2000;

  private abstract static class AbstractRedisTridentDataSource implements ISqlTridentDataSource, Serializable {
    protected abstract StateFactory newStateFactory();
    protected abstract StateUpdater newStateUpdater(RedisStoreMapper storeMapper);

    private final Properties props;
    private final List<FieldInfo> fields;
    private final IOutputSerializer serializer;

    AbstractRedisTridentDataSource(Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
      this.props = props;
      this.fields = fields;
      this.serializer = serializer;
    }

    @Override
    public ITridentDataSource getProducer() {
      throw new UnsupportedOperationException(this.getClass().getName() + " doesn't provide Producer");
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      RedisDataTypeDescription dataTypeDescription = getDataTypeDesc(props);

      RedisStoreMapper storeMapper = new TridentRedisStoreMapper(dataTypeDescription, fields, serializer);

      StateFactory stateFactory = newStateFactory();
      StateUpdater stateUpdater = newStateUpdater(storeMapper);

      return new SimpleSqlTridentConsumer(stateFactory, stateUpdater);
    }

    private RedisDataTypeDescription getDataTypeDesc(Properties props) {
      Preconditions.checkArgument(props.containsKey("data.type"),
              "Redis data source must contain \"data.type\" config");

      RedisDataTypeDescription.RedisDataType dataType = RedisDataTypeDescription.RedisDataType.valueOf(props.getProperty("data.type").toUpperCase());
      String additionalKey = props.getProperty("data.additional.key");

      return new RedisDataTypeDescription(dataType, additionalKey);
    }
  }

  private static class RedisClusterTridentDataSource extends AbstractRedisTridentDataSource {
    private final JedisClusterConfig config;

    RedisClusterTridentDataSource(JedisClusterConfig config, Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
      super(props, fields, serializer);
      this.config = config;
    }

    @Override
    protected StateFactory newStateFactory() {
      return new RedisClusterState.Factory(config);
    }

    @Override
    protected StateUpdater newStateUpdater(RedisStoreMapper storeMapper) {
      return new RedisClusterStateUpdater(storeMapper);
    }
  }

  private static class RedisTridentDataSource extends AbstractRedisTridentDataSource {
    private final JedisPoolConfig config;

    RedisTridentDataSource(JedisPoolConfig config, Properties props, List<FieldInfo> fields, IOutputSerializer serializer) {
      super(props, fields, serializer);
      this.config = config;
    }

    @Override
    protected StateFactory newStateFactory() {
      return new RedisState.Factory(config);
    }

    @Override
    protected StateUpdater newStateUpdater(RedisStoreMapper storeMapper) {
      return new RedisStateUpdater(storeMapper);
    }
  }

  @Override
  public String scheme() {
    return "redis";
  }

  @Override
  public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass, List<FieldInfo> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass, Properties props, List<FieldInfo> fields) {
    Preconditions.checkArgument(JedisURIHelper.isValid(uri), "URI is not valid for Redis: " + uri);

    String host = uri.getHost();
    int port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_REDIS_PORT;
    int dbIdx = JedisURIHelper.getDBIndex(uri);
    String password = JedisURIHelper.getPassword(uri);

    int timeout = Integer.parseInt(props.getProperty("redis.timeout", String.valueOf(DEFAULT_TIMEOUT)));

    boolean clusterMode = Boolean.valueOf(props.getProperty("use.redis.cluster", "false"));

    List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
    IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, props, fieldNames);
    if (clusterMode) {
      JedisClusterConfig config = new JedisClusterConfig.Builder()
              .setNodes(Collections.singleton(new InetSocketAddress(host, port)))
              .setTimeout(timeout)
              .build();
      return new RedisClusterTridentDataSource(config, props, fields, serializer);
    } else {
      JedisPoolConfig config = new JedisPoolConfig(host, port, timeout, password, dbIdx);
      return new RedisTridentDataSource(config, props, fields, serializer);
    }
  }

  private static class TridentRedisStoreMapper implements RedisStoreMapper {
    private final RedisDataTypeDescription dataTypeDescription;
    private final FieldInfo primaryKeyField;
    private final IOutputSerializer outputSerializer;

    private TridentRedisStoreMapper(RedisDataTypeDescription dataTypeDescription, List<FieldInfo> fields, IOutputSerializer outputSerializer) {
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
      String keyFieldName = primaryKeyField.name();
      Object key = tuple.getValueByField(keyFieldName);
      if (key == null) {
        throw new NullPointerException("key field " + keyFieldName + " is null");
      }
      return String.valueOf(key);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
      byte[] array = outputSerializer.write(tuple.getValues(), null).array();
      return new String(array);
    }
  }
}
