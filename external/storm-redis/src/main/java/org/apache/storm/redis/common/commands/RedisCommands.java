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

package org.apache.storm.redis.common.commands;

import java.util.Map;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * This interface represents Jedis methods exhaustively which are used on storm-redis.
 *
 * <p>This is a workaround since Jedis and JedisCluster doesn't implement same interface for binary type of methods, and
 * unify binary methods and string methods into one interface.
 */
public interface RedisCommands {
    // common
    Boolean exists(byte[] key);

    boolean exists(String key);

    Long del(byte[] key);

    Long del(String key);

    String rename(byte[] oldkey, byte[] newkey);

    String rename(String oldkey, String newkey);

    // hash
    byte[] hget(byte[] key, byte[] field);

    Map<byte[], byte[]> hgetAll(byte[] key);

    Map<String, String> hgetAll(String key);

    String hmset(byte[] key, Map<byte[], byte[]> fieldValues);

    String hmset(String key, Map<String, String> fieldValues);

    Long hdel(byte[] key, byte[]... fields);

    ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params);
}
