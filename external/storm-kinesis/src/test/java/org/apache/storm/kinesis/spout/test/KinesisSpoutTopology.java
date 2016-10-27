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
package org.apache.storm.kinesis.spout.test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kinesis.spout.CredentialsProviderChain;
import org.apache.storm.kinesis.spout.ExponentialBackoffRetrier;
import org.apache.storm.kinesis.spout.KinesisConfig;
import org.apache.storm.kinesis.spout.KinesisConnectionInfo;
import org.apache.storm.kinesis.spout.KinesisSpout;
import org.apache.storm.kinesis.spout.RecordToTupleMapper;
import org.apache.storm.kinesis.spout.ZkInfo;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Date;

public class KinesisSpoutTopology {
    public static void main (String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String topologyName = args[0];
        RecordToTupleMapper recordToTupleMapper = new TestRecordToTupleMapper();
        KinesisConnectionInfo kinesisConnectionInfo = new KinesisConnectionInfo(new CredentialsProviderChain(), new ClientConfiguration(), Regions.US_WEST_2,
                1000);
        ZkInfo zkInfo = new ZkInfo("localhost:2181", "/kinesisOffsets", 20000, 15000, 10000L, 3, 2000);
        KinesisConfig kinesisConfig = new KinesisConfig(args[1], ShardIteratorType.TRIM_HORIZON,
                recordToTupleMapper, new Date(), new ExponentialBackoffRetrier(), zkInfo, kinesisConnectionInfo, 10000L);
        KinesisSpout kinesisSpout = new KinesisSpout(kinesisConfig);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", kinesisSpout, 3);
        topologyBuilder.setBolt("bolt", new KinesisBoltTest(), 1).shuffleGrouping("spout");
        Config topologyConfig = new Config();
        topologyConfig.setDebug(true);
        topologyConfig.setNumWorkers(3);
        StormSubmitter.submitTopology(topologyName, topologyConfig, topologyBuilder.createTopology());
    }
}
