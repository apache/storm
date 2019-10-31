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

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.StreamState;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * An example that uses {@link Stream#stateQuery(StreamState)} to query the state
 *
 * <p>You should start a local redis instance before running the 'storm jar' command. By default
 * the connection will be attempted at localhost:6379. The default
 * RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
 *
 * <p><pre>
 * topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
 *  "keySerializerClass":"...", "valueSerializerClass":"...",
 *  "jedisPoolConfig":{"host":"localhost", "port":6379,
 *  "timeout":2000, "database":0, "password":"xyz"}}'
 * </pre>
 */
public class StateQueryExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        StreamState<String, Long> ss = builder.newStream(new TestWordSpout(), new ValueMapper<String>(0), 2)
                                              /*
                                               * Transform the stream of words to a stream of (word, 1) pairs
                                               */
                                              .mapToPair(w -> Pair.of(w, 1))
                                              /*
                                               *  Update the count in the state. Here the first argument 0L is the initial value for the
                                               *  count and
                                               *  the second argument is a function that increments the count for each value received.
                                               */
                                              .updateStateByKey(0L, (count, val) -> count + 1);

        /*
         * A stream of words emitted by the QuerySpout is used as
         * the keys to query the state.
         */
        builder.newStream(new QuerySpout(), new ValueMapper<String>(0))
               /*
                * Queries the state and emits the
                * matching (key, value) as results. The stream state returned
                * by the updateStateByKey is passed as the argument to stateQuery.
                */
               .stateQuery(ss).print();

        Config config = new Config();
        // use redis based state store for persistence
        config.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }

    private static class QuerySpout extends BaseRichSpout {
        private final String[] words = { "nathan", "mike" };
        private SpoutOutputCollector collector;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(2000);
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
}
