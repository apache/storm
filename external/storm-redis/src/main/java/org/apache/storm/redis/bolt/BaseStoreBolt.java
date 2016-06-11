/**
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
package org.apache.storm.redis.bolt;

import com.google.common.base.Preconditions;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

public abstract class BaseStoreBolt extends BaseRedisBolt {
    protected OutputCollector collector;

    protected final RedisStoreMapper mapper;
    protected final RedisDataTypeDescription.RedisDataType dataType;
    protected final String additionalKey;

    public BaseStoreBolt(RedisStoreMapper mapper) {
        this.mapper = mapper;
        Preconditions.checkNotNull(this.mapper, "Mapper is Null");

        RedisDataTypeDescription dataTypeDescription = mapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    @Override
    public void execute(Tuple input) {
        String key = mapper.getKeyFromTuple(input);
        String value = mapper.getValueFromTuple(input);

        JedisCommands jedisCommand = null;
        try {
            jedisCommand = getInstance();

            switch (dataType) {
                case STRING:
                    jedisCommand.set(key, value);
                    break;

                case LIST:
                    jedisCommand.rpush(key, value);
                    break;

                case HASH:
                    jedisCommand.hset(additionalKey, key, value);
                    break;

                case SET:
                    jedisCommand.sadd(key, value);
                    break;

                case SORTED_SET:
                    jedisCommand.zadd(additionalKey, Double.valueOf(value), key);
                    break;

                case HYPER_LOG_LOG:
                    jedisCommand.pfadd(key, value);
                    break;

                case GEO:
                    String[] array = value.split(":");
                    if (array.length != 2) {
                        throw new IllegalArgumentException("value structure should be longitude:latitude");
                    }

                    double longitude = Double.valueOf(array[0]);
                    double latitude = Double.valueOf(array[1]);
                    jedisCommand.geoadd(additionalKey, longitude, latitude, key);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            collector.ack(input);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        } finally {
            returnInstance(jedisCommand);
        }
    }
}
