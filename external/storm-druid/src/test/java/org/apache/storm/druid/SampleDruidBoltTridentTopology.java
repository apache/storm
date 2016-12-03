/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.druid;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.druid.bolt.DruidBeamFactory;
import org.apache.storm.druid.bolt.ITupleDruidEventMapper;
import org.apache.storm.druid.bolt.TupleDruidEventMapper;
import org.apache.storm.druid.trident.DruidBeamStateFactory;
import org.apache.storm.druid.trident.DruidBeamStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Sample application to use Druid Trident bolt.
 *
 * To test this we need to deploy Druid application. Refer Druid quickstart to run druid.
 * http://druid.io/docs/latest/tutorials/quickstart.html
 */
public class SampleDruidBoltTridentTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SampleDruidBoltTridentTopology.class);

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
          throw new IllegalArgumentException("There should be at least one argument. Run as `SampleDruidBoltTridentTopology <zk-url>`");
        }

        TridentTopology tridentTopology = new TridentTopology();
        DruidBeamFactory druidBeamFactory = new SampleDruidBeamFactoryImpl(new HashMap<String, Object>());
        ITupleDruidEventMapper<Map<String, Object>> eventMapper = new TupleDruidEventMapper<>(TupleDruidEventMapper.DEFAULT_FIELD_NAME);

        final Stream stream = tridentTopology.newStream("batch-event-gen", new SimpleBatchSpout(10));

        stream.peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                LOG.info("########### Received tuple: [{}]", input);
            }
        }).partitionPersist(new DruidBeamStateFactory<Map<String, Object>>(druidBeamFactory, eventMapper), new Fields("event"), new DruidBeamStateUpdater());

        Config conf = new Config();



        conf.setDebug(true);
        conf.put("druid.tranquility.zk.connect", args[0]);

        if (args.length > 1) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, tridentTopology.build());
        } else {
            conf.setMaxTaskParallelism(3);

            try (LocalCluster cluster = new LocalCluster();
                 LocalTopology topo = cluster.submitTopology("druid-test", conf, tridentTopology.build());) {
                Thread.sleep(30000);
            }
            System.exit(0);
        }

    }
}
