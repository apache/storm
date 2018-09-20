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

package org.apache.storm.starter;

import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;

public class JoinBoltExample {
    public static void main(String[] args) throws Exception {
        if (!NimbusClient.isLocalOverride()) {
            throw new IllegalStateException("This example only works in local mode.  "
                                            + "Run with storm local not storm jar");
        }
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("genderSpout", genderSpout);
        builder.setSpout("ageSpout", ageSpout);

        // inner join of 'age' and 'gender' records on 'id' field
        JoinBolt joiner = new JoinBolt("genderSpout", "id")
            .join("ageSpout", "id", "genderSpout")
            .select("genderSpout:id,ageSpout:id,gender,age")
            .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));

        builder.setBolt("joiner", joiner)
               .fieldsGrouping("genderSpout", new Fields("id"))
               .fieldsGrouping("ageSpout", new Fields("id"));

        builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("joiner");

        Config conf = new Config();
        StormSubmitter.submitTopologyWithProgressBar("join-example", conf, builder.createTopology());

        generateGenderData(genderSpout);

        generateAgeData(ageSpout);
    }

    private static void generateAgeData(FeederSpout ageSpout) {
        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }
    }

    private static void generateGenderData(FeederSpout genderSpout) {
        for (int i = 0; i < 10; i++) {
            String gender;
            if (i % 2 == 0) {
                gender = "male";
            } else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }
    }
}
