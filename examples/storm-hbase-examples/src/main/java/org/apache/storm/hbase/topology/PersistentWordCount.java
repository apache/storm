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

package org.apache.storm.hbase.topology;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";


    public static void main(String[] args) throws Exception {
        Config config = new Config();

        Map<String, Object> hbConf = new HashMap<>();
        if (args.length > 0) {
            hbConf.put("hbase.rootdir", args[0]);
        }
        config.put("hbase.conf", hbConf);

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
            .withRowKeyField("word")
            .withColumnFields(new Fields("word"))
            .withCounterFields(new Fields("count"))
            .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("WordCount", mapper)
            .withConfigKey("hbase.conf");


        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, hbase, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        String topoName = "test";
        if (args.length == 2) {
            topoName = args[0];
        } else if (args.length == 4) {
            System.out.println("hdfs url: " + args[0]
                    + ", keytab file: " + args[2]
                    + ", principal name: " + args[3]
                    + ", toplogy name: " + args[1]);
            hbConf.put(HBaseSecurityUtil.STORM_KEYTAB_FILE_KEY, args[2]);
            hbConf.put(HBaseSecurityUtil.STORM_USER_NAME_KEY, args[3]);
            config.setNumWorkers(3);
            topoName = args[1];
        } else if (args.length != 1) {
            System.out.println("Usage: PersistentWordCount <hbase.rootdir> [topology name] [keytab file] [principal name]");
            return;
        }
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }
}
