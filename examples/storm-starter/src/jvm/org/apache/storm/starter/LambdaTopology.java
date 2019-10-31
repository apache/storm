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

import java.io.Serializable;
import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;

public class LambdaTopology extends ConfigurableTopology {
    public static void main(String[] args) {
        ConfigurableTopology.start(new LambdaTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // example. spout1: generate random strings
        // bolt1: get the first part of a string
        // bolt2: output the tuple

        // NOTE: Variable used in lambda expression should be final or effectively final
        // (or it will cause compilation error),
        // and variable type should implement the Serializable interface if it isn't primitive type
        // (or it will cause not serializable exception).
        Prefix prefix = new Prefix("Hello lambda:");
        String suffix = ":so cool!";
        int tag = 999;

        builder.setSpout("spout1", () -> UUID.randomUUID().toString());
        builder.setBolt("bolt1", (tuple, collector) -> {
            String[] parts = tuple.getStringByField("lambda").split("\\-");
            collector.emit(new Values(prefix + parts[0] + suffix, tag));
        }, "strValue", "intValue").shuffleGrouping("spout1");
        builder.setBolt("bolt2", tuple -> System.out.println(tuple)).shuffleGrouping("bolt1");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        return submit("lambda-demo", conf, builder);
    }
}
