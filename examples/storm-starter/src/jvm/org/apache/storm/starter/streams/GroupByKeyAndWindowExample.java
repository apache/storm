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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * An example that shows the usage of {@link PairStream#groupByKeyAndWindow(Window)}
 * and {@link PairStream#reduceByKeyAndWindow(Reducer, Window)}.
 */
public class GroupByKeyAndWindowExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();

        // a stream of stock quotes
        builder.newStream(new StockQuotes(), new PairValueMapper<String, Double>(0, 1))
               /*
                * The elements having the same key within the window will be grouped
                * together and the corresponding values will be merged.
                *
                * The result is a PairStream<String, Iterable<Double>> with
                * 'stock symbol' as the key and 'stock prices' for that symbol within the window as the value.
                */
               .groupByKeyAndWindow(SlidingWindows.of(Count.of(6), Count.of(3)))
               .print();

        // a stream of stock quotes
        builder.newStream(new StockQuotes(), new PairValueMapper<String, Double>(0, 1))
               /*
                * The elements having the same key within the window will be grouped
                * together and their values will be reduced using the given reduce function.
                *
                * Here the result is a PairStream<String, Double> with
                * 'stock symbol' as the key and the maximum price for that symbol within the window as the value.
                */
               .reduceByKeyAndWindow((x, y) -> x > y ? x : y, SlidingWindows.of(Count.of(6), Count.of(3)))
               .print();

        Config config = new Config();
        String topoName = GroupByKeyAndWindowExample.class.getName();
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }

    private static class StockQuotes extends BaseRichSpout {
        private final List<List<Values>> values = Arrays.asList(
            Arrays.asList(new Values("AAPL", 100.0), new Values("GOOG", 780.0), new Values("FB", 125.0)),
            Arrays.asList(new Values("AAPL", 105.0), new Values("GOOG", 790.0), new Values("FB", 130.0)),
            Arrays.asList(new Values("AAPL", 102.0), new Values("GOOG", 788.0), new Values("FB", 128.0))
        );
        private SpoutOutputCollector collector;
        private int index = 0;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(5000);
            for (Values v : values.get(index)) {
                collector.emit(v);
            }
            index = (index + 1) % values.size();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("symbol", "price"));
        }
    }
}
