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

import java.util.List;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.JedisCommands;

/**
 * Basic bolt for querying from Redis and filters out if key/field doesn't exist.
 * If key/field exists on Redis, this bolt just forwards input tuple to default stream.
 *
 * <p>Supported data types: STRING, HASH, SET, SORTED_SET, HYPER_LOG_LOG, GEO.
 *
 * <p>Note: For STRING it checks such key exists on the key space.
 * For HASH and SORTED_SET and GEO, it checks such field exists on that data structure.
 * For SET and HYPER_LOG_LOG, it check such value exists on that data structure.
 * (Note that it still refers key from tuple via RedisFilterMapper#getKeyFromTuple())
 * In order to apply checking this to SET, you need to input additional key this case.
 *
 * <p>Note2: If you want to just query about existence of key regardless of actual data type,
 * specify STRING to data type of RedisFilterMapper.
 */
public class RedisFilterBolt extends AbstractRedisBolt {
    private final RedisFilterMapper filterMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;

    /**
     * Constructor for single Redis environment (JedisPool).
     * @param config configuration for initializing JedisPool
     * @param filterMapper mapper containing which datatype, query key that Bolt uses
     */
    public RedisFilterBolt(JedisPoolConfig config, RedisFilterMapper filterMapper) {
        super(config);

        this.filterMapper = filterMapper;

        RedisDataTypeDescription dataTypeDescription = filterMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();

        if (dataType == RedisDataTypeDescription.RedisDataType.SET
                && additionalKey == null) {
            throw new IllegalArgumentException("additionalKey should be defined");
        }
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster).
     * @param config configuration for initializing JedisCluster
     * @param filterMapper mapper containing which datatype, query key that Bolt uses
     */
    public RedisFilterBolt(JedisClusterConfig config, RedisFilterMapper filterMapper) {
        super(config);

        this.filterMapper = filterMapper;

        RedisDataTypeDescription dataTypeDescription = filterMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Tuple input) {
        String key = filterMapper.getKeyFromTuple(input);

        boolean found;
        JedisCommands jedisCommand = null;
        try {
            jedisCommand = getInstance();

            switch (dataType) {
                case STRING:
                    found = jedisCommand.exists(key);
                    break;

                case SET:
                    found = jedisCommand.sismember(additionalKey, key);
                    break;

                case HASH:
                    found = jedisCommand.hexists(additionalKey, key);
                    break;

                case SORTED_SET:
                    found = jedisCommand.zrank(additionalKey, key) != null;
                    break;

                case HYPER_LOG_LOG:
                    found = jedisCommand.pfcount(key) > 0;
                    break;

                case GEO:
                    List<GeoCoordinate> geopos = jedisCommand.geopos(additionalKey, key);
                    found = (geopos != null && geopos.size() > 0);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            if (found) {
                collector.emit(input, input.getValues());
            }

            collector.ack(input);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        } finally {
            returnInstance(jedisCommand);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        filterMapper.declareOutputFields(declarer);
    }
}
