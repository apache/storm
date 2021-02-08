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

package org.apache.storm.mongodb.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.mongodb.bolt.MongoLookupBolt;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoLookupMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoLookupMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class LookupWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String LOOKUP_BOLT = "LOOKUP_BOLT";
    private static final String TOTAL_COUNT_BOLT = "TOTAL_COUNT_BOLT";

    private static final String TEST_MONGODB_URL = "mongodb://127.0.0.1:27017/test";
    private static final String TEST_MONGODB_COLLECTION_NAME = "wordcount";

    public static void main(String[] args) throws Exception {
        String url = TEST_MONGODB_URL;
        String collectionName = TEST_MONGODB_COLLECTION_NAME;

        if (args.length >= 2) {
            url = args[0];
            collectionName = args[1];
        }

        WordSpout spout = new WordSpout();
        TotalWordCounter totalBolt = new TotalWordCounter();

        MongoLookupMapper mapper = new SimpleMongoLookupMapper()
                .withFields("word", "count");

        QueryFilterCreator filterCreator = new SimpleQueryFilterCreator()
                .withField("word");

        MongoLookupBolt lookupBolt = new MongoLookupBolt(url, collectionName, filterCreator, mapper);

        //wordspout -> lookupbolt -> totalCountBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(LOOKUP_BOLT, lookupBolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(TOTAL_COUNT_BOLT, totalBolt, 1).fieldsGrouping(LOOKUP_BOLT, new Fields("word"));

        String topoName = "test";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3) {
            System.out.println("Usage: LookupWordCount <mongodb url> <mongodb collection> [topology name]");
            return;
        }

        Config config = new Config();
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }
}
