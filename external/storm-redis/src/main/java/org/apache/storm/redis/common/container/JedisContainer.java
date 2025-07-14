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

package org.apache.storm.redis.common.container;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.JedisCommands;

/**
 * Adapter for providing a unified interface for running commands over both Jedis and JedisCluster instances.
 */
public class JedisContainer implements JedisCommandsContainer {
    private static final Logger LOG = LoggerFactory.getLogger(JedisContainer.class);

    private JedisPool jedisPool;

    /**
     * Constructor.
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public JedisContainer(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private <T> T runCommand(Function<JedisCommands, T> command) {
        final JedisCommands jedisCommands = jedisPool.getResource();
        try {
            return command.apply(jedisCommands);
        } finally {
            try {
                ((Closeable) jedisCommands).close();
            } catch (IOException e) {
                LOG.error("Failed to close (return) instance to pool");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        jedisPool.close();
    }

    @Override
    public Boolean exists(final String key) {
        return runCommand((jedisCommands) -> jedisCommands.exists(key));
    }

    @Override
    public String get(final String key) {
        return runCommand((jedisCommands) -> jedisCommands.get(key));
    }

    @Override
    public String hget(final String key, final String field) {
        return runCommand((jedisCommands) -> jedisCommands.hget(key, field));
    }

    @Override
    public Long geoadd(final String key, final double longitude, final double latitude, final String member) {
        return runCommand((jedisCommands) -> jedisCommands.geoadd(key, longitude, latitude, member));
    }

    @Override
    public List<GeoCoordinate> geopos(final String key, final String... members) {
        return runCommand((jedisCommands) -> jedisCommands.geopos(key, members));
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return runCommand((jedisCommands) -> jedisCommands.hexists(key, field));
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return runCommand((jedisCommands) -> jedisCommands.hset(key, field, value));
    }

    @Override
    public String lpop(final String key) {
        return runCommand((jedisCommands) -> jedisCommands.lpop(key));
    }

    @Override
    public Long pfadd(final String key, final String... elements) {
        return runCommand((jedisCommands) -> jedisCommands.pfadd(key, elements));
    }

    @Override
    public long pfcount(final String key) {
        return runCommand((jedisCommands) -> jedisCommands.pfcount(key));
    }

    @Override
    public Long rpush(final String key, final String... string) {
        return runCommand((jedisCommands) -> jedisCommands.rpush(key, string));
    }

    @Override
    public Long sadd(final String key, final String... member) {
        return runCommand((jedisCommands) -> jedisCommands.sadd(key, member));
    }

    @Override
    public Long scard(final String key) {
        return runCommand((jedisCommands) -> jedisCommands.scard(key));
    }

    @Override
    public String set(final String key, final String value) {
        return runCommand((jedisCommands) -> jedisCommands.set(key, value));
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return runCommand((jedisCommands) -> jedisCommands.sismember(key, member));
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        return runCommand((jedisCommands) -> jedisCommands.zadd(key, score, member));
    }

    @Override
    public Long zrank(final String key, final String member) {
        return runCommand((jedisCommands) -> jedisCommands.zrank(key, member));
    }

    @Override
    public Double zscore(final String key, final String member) {
        return runCommand((jedisCommands) -> jedisCommands.zscore(key, member));
    }
}
