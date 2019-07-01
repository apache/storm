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

package org.apache.storm.redis.common.container;

import java.io.Closeable;
import java.io.IOException;
import org.apache.storm.redis.common.adapter.RedisCommandsAdapterJedis;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

public class RedisContainer implements RedisCommandsInstanceContainer {
    private static final Logger LOG = LoggerFactory.getLogger(JedisContainer.class);

    private JedisPool jedisPool;

    /**
     * Constructor.
     *
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public RedisContainer(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisCommands getInstance() {
        return new RedisCommandsAdapterJedis(jedisPool.getResource());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnInstance(RedisCommands redisCommands) {
        if (redisCommands == null) {
            return;
        }

        try {
            ((Closeable) redisCommands).close();
        } catch (IOException e) {
            LOG.error("Failed to close (return) instance to pool");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        jedisPool.close();
    }
}
