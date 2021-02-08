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

package org.apache.storm.redis.topology;

import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisFilterBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WhitelistWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String WHITELIST_BOLT = "WHITELIST_BOLT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    private static final String TEST_REDIS_HOST = "127.0.0.1";
    private static final int TEST_REDIS_PORT = 6379;

    public static class PrintWordTotalCountBolt extends BaseRichBolt {
        private static final Logger LOG = LoggerFactory.getLogger(PrintWordTotalCountBolt.class);
        private static final Random RANDOM = new Random();
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String wordName = input.getStringByField("word");
            String countStr = input.getStringByField("count");

            // print lookup result with low probability
            if (RANDOM.nextInt(1000) > 995) {
                int count = 0;
                if (countStr != null) {
                    count = Integer.parseInt(countStr);
                }
                LOG.info("Count result - word : " + wordName + " / count : " + count);
            }

            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static void main(String[] args) throws Exception {
        String host = TEST_REDIS_HOST;
        int port = TEST_REDIS_PORT;

        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();

        WordSpout spout = new WordSpout();
        RedisFilterMapper filterMapper = setupWhitelistMapper();
        RedisFilterBolt whitelistBolt = new RedisFilterBolt(poolConfig, filterMapper);
        WordCounter wordCounterBolt = new WordCounter();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(WHITELIST_BOLT, whitelistBolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(COUNT_BOLT, wordCounterBolt, 1).fieldsGrouping(WHITELIST_BOLT, new Fields("word"));
        PrintWordTotalCountBolt printBolt = new PrintWordTotalCountBolt();
        builder.setBolt(PRINT_BOLT, printBolt, 1).shuffleGrouping(COUNT_BOLT);

        String topoName = "test";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3) {
            System.out.println("Usage: WhitelistWordCount <redis host> <redis port> [topology name]");
            return;
        }
        Config config = new Config();
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }

    private static RedisFilterMapper setupWhitelistMapper() {
        return new WhitelistWordFilterMapper();
    }

    private static class WhitelistWordFilterMapper implements RedisFilterMapper {
        private RedisDataTypeDescription description;
        private final String setKey = "whitelist";

        WhitelistWordFilterMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.SET, setKey);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return null;
        }
    }
}
