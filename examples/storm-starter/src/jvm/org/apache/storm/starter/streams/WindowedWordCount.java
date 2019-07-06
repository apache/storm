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
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * A windowed word count example.
 */
public class WindowedWordCount {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        // A stream of random sentences
        builder.newStream(new RandomSentenceSpout(), new ValueMapper<String>(0), 2)
               /*
                * a two seconds tumbling window
                */
               .window(TumblingWindows.of(Duration.seconds(2)))
               /*
                * split the sentences to words
                */
               .flatMap(s -> Arrays.asList(s.split(" ")))
               /*
                * create a stream of (word, 1) pairs
                */
               .mapToPair(w -> Pair.of(w, 1))
               /*
                * compute the word counts in the last two second window
                */
               .countByKey()
               /*
                * emit the count for the words that occurred
                * at-least five times in the last two seconds
                */
               .filter(x -> x.getSecond() >= 5)
               /*
                * print the results to stdout
                */
               .print();

        Config config = new Config();
        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }
}
