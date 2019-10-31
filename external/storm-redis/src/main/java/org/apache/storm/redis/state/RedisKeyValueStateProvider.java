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

package org.apache.storm.redis.state;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.apache.storm.state.State;
import org.apache.storm.state.StateProvider;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides {@link RedisKeyValueState}.
 */
public class RedisKeyValueStateProvider implements StateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyValueStateProvider.class);

    @Override
    public State newState(String namespace, Map<String, Object> topoConf, TopologyContext context) {
        try {
            return getRedisKeyValueState(namespace, topoConf, context, getStateConfig(topoConf));
        } catch (Exception ex) {
            LOG.error("Error loading config from storm conf {}", topoConf);
            throw new RuntimeException(ex);
        }
    }

    StateConfig getStateConfig(Map<String, Object> topoConf) throws Exception {
        StateConfig stateConfig;
        String providerConfig;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        if (topoConf.containsKey(Config.TOPOLOGY_STATE_PROVIDER_CONFIG)) {
            providerConfig = (String) topoConf.get(Config.TOPOLOGY_STATE_PROVIDER_CONFIG);
            stateConfig = mapper.readValue(providerConfig, StateConfig.class);
        } else {
            stateConfig = new StateConfig();
        }
        return stateConfig;
    }

    private RedisKeyValueState getRedisKeyValueState(String namespace, Map<String, Object> topoConf, TopologyContext context,
                                                     StateConfig config) throws Exception {
        JedisPoolConfig jedisPoolConfig = getJedisPoolConfig(config);
        JedisClusterConfig jedisClusterConfig = getJedisClusterConfig(config);

        if (jedisPoolConfig == null && jedisClusterConfig == null) {
            jedisPoolConfig = buildDefaultJedisPoolConfig();
        }

        if (jedisPoolConfig != null) {
            return new RedisKeyValueState(namespace, jedisPoolConfig,
                                          getKeySerializer(topoConf, context, config), getValueSerializer(topoConf, context, config));
        } else {
            return new RedisKeyValueState(namespace, jedisClusterConfig,
                                          getKeySerializer(topoConf, context, config), getValueSerializer(topoConf, context, config));
        }
    }

    private Serializer getKeySerializer(Map<String, Object> topoConf, TopologyContext context, StateConfig config) throws Exception {
        Serializer serializer;
        if (config.keySerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.keySerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.keyClass != null) {
            serializer = new DefaultStateSerializer(topoConf, context, Collections.singletonList(Class.forName(config.keyClass)));
        } else {
            serializer = new DefaultStateSerializer(topoConf, context);
        }
        return serializer;
    }

    private Serializer getValueSerializer(Map<String, Object> topoConf, TopologyContext context, StateConfig config) throws Exception {
        Serializer serializer;
        if (config.valueSerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.valueSerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.valueClass != null) {
            serializer = new DefaultStateSerializer(topoConf, context, Collections.singletonList(Class.forName(config.valueClass)));
        } else {
            serializer = new DefaultStateSerializer(topoConf, context);
        }
        return serializer;
    }

    private JedisPoolConfig getJedisPoolConfig(StateConfig config) {
        return config.jedisPoolConfig;
    }

    private JedisClusterConfig getJedisClusterConfig(StateConfig config) {
        return config.jedisClusterConfig;
    }

    private JedisPoolConfig buildDefaultJedisPoolConfig() {
        return new JedisPoolConfig.Builder().build();
    }

    public static class StateConfig {
        public String keyClass;
        public String valueClass;
        public String keySerializerClass;
        public String valueSerializerClass;
        public JedisPoolConfig jedisPoolConfig;
        public JedisClusterConfig jedisClusterConfig;

        @Override
        public String toString() {
            return "StateConfig{"
                    + "keyClass='" + keyClass + '\''
                    + ", valueClass='" + valueClass + '\''
                    + ", keySerializerClass='" + keySerializerClass + '\''
                    + ", valueSerializerClass='" + valueSerializerClass + '\''
                    + ", jedisPoolConfig=" + jedisPoolConfig
                    + ", jedisClusterConfig=" + jedisClusterConfig
                    + '}';
        }
    }

}
