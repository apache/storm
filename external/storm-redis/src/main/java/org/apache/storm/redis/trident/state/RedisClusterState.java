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

package org.apache.storm.redis.trident.state;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisCluster;

/**
 * Implementation of State for Redis Cluster environment.
 */
public class RedisClusterState implements State {
    private JedisCluster jedisCluster;

    /**
     * Constructor.
     *
     * @param jedisCluster JedisCluster
     */
    public RedisClusterState(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beginCommit(Long someLong) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(Long someLong) {
    }

    /**
     * Borrows JedisCluster instance.
     *
     * <p>Note that you should return borrowed instance when you finish using instance.
     *
     * @return JedisCluster instance
     */
    public JedisCluster getJedisCluster() {
        return this.jedisCluster;
    }

    /**
     * Returns JedisCluster instance to pool.
     *
     * @param jedisCluster JedisCluster instance to return to pool
     */
    public void returnJedisCluster(JedisCluster jedisCluster) {
        //do nothing
    }

    /**
     * RedisClusterState.Factory implements StateFactory for Redis Cluster environment.
     *
     * @see StateFactory
     */
    public static class Factory implements StateFactory {
        public static final GenericObjectPoolConfig<Connection> DEFAULT_POOL_CONFIG = new GenericObjectPoolConfig<>();

        private final JedisClusterConfig jedisClusterConfig;

        /**
         * Constructor.
         *
         * @param config configuration of JedisCluster
         */
        public Factory(JedisClusterConfig config) {
            this.jedisClusterConfig = config;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            final JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(),
                                                         jedisClusterConfig.getTimeout(),
                                                         jedisClusterConfig.getTimeout(),
                                                         jedisClusterConfig.getMaxRedirections(),
                                                         jedisClusterConfig.getPassword(),
                                                         DEFAULT_POOL_CONFIG);

            return new RedisClusterState(jedisCluster);
        }
    }

}
