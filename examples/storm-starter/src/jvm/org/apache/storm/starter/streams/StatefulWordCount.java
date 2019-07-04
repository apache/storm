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
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.StateUpdater;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * A stateful word count that uses {@link PairStream#updateStateByKey(StateUpdater)} to
 * save the counts in a key value state. This example uses Redis state store.
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
public class StatefulWordCount {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        // a stream of words
        builder.newStream(new TestWordSpout(), new ValueMapper<String>(0), 2)
               .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(2)))
               /*
                * create a stream of (word, 1) pairs
                */
               .mapToPair(w -> Pair.of(w, 1))
               /*
                * compute the word counts in the last two second window
                */
               .countByKey()
               /*
                * update the word counts in the state.
                * Here the first argument 0L is the initial value for the state
                * and the second argument is a function that adds the count to the current value in the state.
                */
               .updateStateByKey(0L, (state, count) -> state + count)
               /*
                * convert the state back to a stream and print the results
                */
               .toPairStream()
               .print();

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
}
