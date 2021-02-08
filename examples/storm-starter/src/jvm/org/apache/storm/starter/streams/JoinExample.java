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
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * An example that demonstrates the usage of {@link PairStream#join(PairStream)} to join
 * multiple streams.
 */
public class JoinExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        // a stream of (number, square) pairs
        PairStream<Integer, Integer> squares = builder
            .newStream(new NumberSpout(x -> x * x),
                       new PairValueMapper<>(0, 1));
        // a stream of (number, cube) pairs
        PairStream<Integer, Integer> cubes = builder
            .newStream(new NumberSpout(x -> x * x * x),
                       new PairValueMapper<>(0, 1));

        // create a windowed stream of five seconds duration
        squares.window(TumblingWindows.of(Duration.seconds(5)))
               /*
                * Join the squares and the cubes stream within the window.
                * The values in the squares stream having the same key as that
                * of the cubes stream within the window will be joined together.
                */
               .join(cubes)
               /**
                * The results should be of the form (number, (square, cube))
                */
               .print();

        Config config = new Config();
        String topoName = JoinExample.class.getName();
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }

    private static class NumberSpout extends BaseRichSpout {
        private final Function<Integer, Integer> function;
        private SpoutOutputCollector collector;
        private int count = 1;

        NumberSpout(Function<Integer, Integer> function) {
            this.function = function;
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(990);
            collector.emit(new Values(count, function.apply(count)));
            count++;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "val"));
        }
    }

}
