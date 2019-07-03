/*
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

package org.apache.storm.redis.common.adapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.redis.common.commands.RedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * Adapter class to make Jedis instance play with BinaryRedisCommands interface.
 */
public class RedisCommandsAdapterJedis implements RedisCommands, Closeable {
    private Jedis jedis;

    public RedisCommandsAdapterJedis(Jedis resource) {
        jedis = resource;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return jedis.hget(key, field);
    }

    @Override
    public Boolean exists(byte[] key) {
        return jedis.exists(key);
    }

    @Override
    public boolean exists(String key) {
        return jedis.exists(key);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> fieldValues) {
        return jedis.hmset(key, fieldValues);
    }

    @Override
    public String hmset(String key, Map<String, String> fieldValues) {
        return jedis.hmset(key, fieldValues);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return jedis.hgetAll(key);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        return jedis.hdel(key, fields);
    }

    @Override
    public Long del(byte[] key) {
        return jedis.del(key);
    }

    @Override
    public Long del(String key) {
        return jedis.del(key);
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return jedis.rename(oldkey, newkey);
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return jedis.rename(oldkey, newkey);
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        return jedis.hscan(key, cursor, params);
    }

    @Override
    public void close() throws IOException {
        jedis.close();
    }
}
