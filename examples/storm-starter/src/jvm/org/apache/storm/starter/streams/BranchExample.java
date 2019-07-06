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
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example that demonstrates the usage of {@link Stream#branch(Predicate[])} to split a stream
 * into multiple branches based on predicates.
 */
public class BranchExample {
    private static final Logger LOG = LoggerFactory.getLogger(BranchExample.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        Stream<Integer>[] evenAndOdd = builder
                /*
                 * Create a stream of random numbers from a spout that
                 * emits random integers by extracting the tuple value at index 0.
                 */
                .newStream(new RandomIntegerSpout(), new ValueMapper<Integer>(0))
                /*
                 * Split the stream of numbers into streams of
                 * even and odd numbers. The first stream contains even
                 * and the second contains odd numbers.
                 */
                .branch(x -> (x % 2) == 0,
                    x -> (x % 2) == 1);

        evenAndOdd[0].forEach(x -> LOG.info("EVEN> " + x));
        evenAndOdd[1].forEach(x -> LOG.info("ODD > " + x));

        Config config = new Config();
        String topoName = "branchExample";
        if (args.length > 0) {
            topoName = args[0];
        }

        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.build());
    }

}
