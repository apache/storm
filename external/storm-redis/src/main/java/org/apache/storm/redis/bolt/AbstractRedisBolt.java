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

package org.apache.storm.redis.bolt;

import java.util.Map;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.JedisCommandsContainer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;

/**
 * AbstractRedisBolt class is for users to implement custom bolts which makes interaction with Redis.
 *
 * <p>Due to environment abstraction, AbstractRedisBolt provides JedisCommands which contains only single key operations.
 *
 * <p>Custom Bolts may want to follow this pattern:
 *
 * <p><blockquote><pre>
 * JedisCommands jedisCommands = null;
 * try {
 *     jedisCommand = getInstance();
 *     // do some works
 * } finally {
 *     if (jedisCommand != null) {
 *         returnInstance(jedisCommand);
 *     }
 * }
 * </pre></blockquote>
 *
 */
public abstract class AbstractRedisBolt extends BaseTickTupleAwareRichBolt {
    protected OutputCollector collector;

    private transient JedisCommandsContainer container;

    private JedisPoolConfig jedisPoolConfig;
    private JedisClusterConfig jedisClusterConfig;

    /**
     * Constructor for single Redis environment (JedisPool).
     *
     * @param config configuration for initializing JedisPool
     */
    public AbstractRedisBolt(JedisPoolConfig config) {
        this.jedisPoolConfig = config;
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster).
     *
     * @param config configuration for initializing JedisCluster
     */
    public AbstractRedisBolt(JedisClusterConfig config) {
        this.jedisClusterConfig = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        // FIXME: stores map (topoConf), topologyContext and expose these to derived classes
        this.collector = collector;

        if (jedisPoolConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisPoolConfig);
        } else if (jedisClusterConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisClusterConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Borrow JedisCommands instance from container.<p/>
     * JedisCommands is an interface which contains single key operations.
     * @return implementation of JedisCommands
     * @see JedisCommandsContainer
     */
    protected JedisCommandsContainer getInstance() {
        return this.container;
    }

    @Override
    public void cleanup() {
        container.close();
    }
}
