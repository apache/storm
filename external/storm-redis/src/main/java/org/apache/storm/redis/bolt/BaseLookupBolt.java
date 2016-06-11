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
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisCommands;

import java.util.List;

public abstract class BaseLookupBolt extends BaseRedisBolt {
    protected OutputCollector collector;

    protected final RedisLookupMapper mapper;
    protected final RedisDataTypeDescription.RedisDataType dataType;
    protected final String additionalKey;

    public BaseLookupBolt(RedisLookupMapper lookupMapper) {
        this.mapper = lookupMapper;
        Preconditions.checkNotNull(this.mapper, "Mapper is Null");

        RedisDataTypeDescription dataTypeDescription = lookupMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    @Override
    public void execute(Tuple input) {
        String key = mapper.getKeyFromTuple(input);
        Object lookupValue;

        JedisCommands jedisCommand = getInstance();
        try {
            switch (dataType) {
                case STRING:
                    lookupValue = jedisCommand.get(key);
                    break;

                case LIST:
                    lookupValue = jedisCommand.lpop(key);
                    break;

                case HASH:
                    lookupValue = jedisCommand.hget(additionalKey, key);
                    break;

                case SET:
                    lookupValue = jedisCommand.scard(key);
                    break;

                case SORTED_SET:
                    lookupValue = jedisCommand.zscore(additionalKey, key);
                    break;

                case HYPER_LOG_LOG:
                    lookupValue = jedisCommand.pfcount(key);
                    break;

                case GEO:
                    lookupValue = jedisCommand.geopos(additionalKey, key);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            List<Values> values = mapper.toTuple(input, lookupValue);
            for (Values value : values) {
                collector.emit(input, value);
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
