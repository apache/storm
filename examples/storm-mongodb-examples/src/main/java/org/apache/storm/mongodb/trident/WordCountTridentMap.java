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

package org.apache.storm.mongodb.trident;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.mongodb.trident.state.MongoMapState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountTridentMap {

    public static StormTopology buildTopology(String url, String collectionName) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word", "count");

        MongoMapState.Options options = new MongoMapState.Options();
        options.url = url;
        options.collectionName = collectionName;
        options.mapper = mapper;
        QueryFilterCreator filterCreator = new SimpleQueryFilterCreator()
                .withField("word");
        options.queryCreator = filterCreator;

        StateFactory factory = MongoMapState.transactional(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        TridentState state = stream.groupBy(new Fields("word"))
                .persistentAggregate(factory, new Fields("count"), new Sum(), new Fields("sum"));

        stream.stateQuery(state, new Fields("word"), new MapGet(), new Fields("sum"))
                .each(new Fields("word", "sum"), new PrintFunction(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        String topoName = "wordCounter";
        if (args.length == 3) {
            topoName = args[2];
        } else if (args.length > 3 || args.length < 2) {
            System.out.println("Usage: WordCountTrident <mongodb url> <mongodb collection> [topology name]");
            return;
        }
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology(topoName, conf, buildTopology(args[0], args[1]));
    }

}
