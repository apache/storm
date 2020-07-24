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

import java.io.Closeable;
import java.util.List;


/**
 * Interfaces for containers which stores instances implementing JedisCommands.
 */
public interface JedisCommandsContainer extends Closeable {
    Boolean exists(String key);

    String get(String key);

    String hget(String key, String field);

    Long geoadd(String key, double longitude, double latitude, String member);

    List<GeoCoordinate> geopos(String key, String... members);

    Boolean hexists(String key, String field);

    Long hset(String key, String field, String value);

    String lpop(String key);

    Long pfadd(String key, String... elements);

    long pfcount(String key);

    Long rpush(String key, String... string);

    Long sadd(String key, String... member);

    Long scard(String key);

    String set(String key, String value);

    Boolean sismember(String key, String member);

    Long zadd(String key, double score, String member);

    Long zrank(String key, String member);

    Double zscore(String key, String member);







    /**
     * Release Container.
     */
    @Override
    void close();
}
