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

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.JedisCluster;

import java.util.List;

/**
 * Container for managing JedisCluster.
 * <p/>
 * Note that JedisCluster doesn't need to be pooled since it's thread-safe and it stores pools internally.
 */
public class JedisClusterContainer implements JedisCommandsContainer {

    private JedisCluster jedisCluster;

    /**
     * Constructor.
     * @param jedisCluster JedisCluster instance
     */
    public JedisClusterContainer(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public Boolean exists(final String key) {
        return jedisCluster.exists(key);
    }

    @Override
    public String get(final String key) {
        return jedisCluster.get(key);
    }

    @Override
    public String hget(final String key, final String field) {
        return jedisCluster.hget(key, field);
    }

    @Override
    public Long geoadd(final String key, final double longitude, final double latitude, final String member) {
        return jedisCluster.geoadd(key, longitude, latitude, member);
    }

    @Override
    public List<GeoCoordinate> geopos(final String key, final String... members) {
        return jedisCluster.geopos(key, members);
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return jedisCluster.hexists(key, field);
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return jedisCluster.hset(key, field, value);
    }

    @Override
    public String lpop(final String key) {
        return jedisCluster.lpop(key);
    }

    @Override
    public Long pfadd(final String key, final String... elements) {
        return jedisCluster.pfadd(key, elements);
    }

    @Override
    public long pfcount(final String key) {
        return jedisCluster.pfcount(key);
    }

    @Override
    public Long rpush(final String key, final String... string) {
        return jedisCluster.rpush(key, string);
    }

    @Override
    public Long sadd(final String key, final String... member) {
        return jedisCluster.sadd(key, member);
    }

    @Override
    public Long scard(final String key) {
        return jedisCluster.scard(key);
    }

    @Override
    public String set(final String key, final String value) {
        return jedisCluster.set(key, value);
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return jedisCluster.sismember(key, member);
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        return jedisCluster.zadd(key, score, member);
    }

    @Override
    public Long zrank(final String key, final String member) {
        return jedisCluster.zrank(key, member);
    }

    @Override
    public Double zscore(final String key, final String member) {
        return jedisCluster.zscore(key, member);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.jedisCluster.close();
    }
}
