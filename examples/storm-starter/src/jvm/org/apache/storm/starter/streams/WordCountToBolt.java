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

package org.apache.storm.starter.streams;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.ITuple;

/**
 * An example that computes word counts and finally emits the results to an
 * external bolt (sink).
 */
public class WordCountToBolt {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();

        // Redis config parameters for the RedisStoreBolt
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
            .setHost("127.0.0.1").setPort(6379).build();
        // Storm tuple to redis key-value mapper
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        // The redis bolt (sink)
        IRichBolt redisStoreBolt = new RedisStoreBolt(poolConfig, storeMapper);

        // A stream of words
        builder.newStream(new TestWordSpout(), new ValueMapper<String>(0))
               /*
                * create a stream of (word, 1) pairs
                */
               .mapToPair(w -> Pair.of(w, 1))
               /*
                * aggregate the count
                */
               .countByKey()
               /*
                * The result of aggregation is forwarded to
                * the RedisStoreBolt. The forwarded tuple is a
                * key-value pair of (word, count) with ("key", "value")
                * being the field names.
                */
               .to(redisStoreBolt);

        Config config = new Config();
        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }

    // Maps a storm tuple to redis key and value
    private static class WordCountStoreMapper implements RedisStoreMapper {
        private final RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        WordCountStoreMapper() {
            description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("key");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return String.valueOf(tuple.getLongByField("value"));
        }
    }
}
