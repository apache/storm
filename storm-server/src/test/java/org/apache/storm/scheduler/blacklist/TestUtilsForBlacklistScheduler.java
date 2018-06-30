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
package org.apache.storm.scheduler.blacklist;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SchedulerTestUtils;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TestUtilsForBlacklistScheduler extends SchedulerTestUtils{

    private static final Logger LOG = LoggerFactory.getLogger(TestUtilsForBlacklistScheduler.class);

    public static Map<String, SupervisorDetails> removeSupervisorFromSupervisors(Map<String, SupervisorDetails> supervisorDetailsMap, String supervisor) {
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        retList.putAll(supervisorDetailsMap);
        retList.remove(supervisor);
        return retList;
    }

    public static TopologyDetails getTopology(String name, Map<String, Object> config, int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism, int launchTime, boolean blacklistEnable) {

        Config conf = new Config();
        conf.putAll(config);
        conf.put(Config.TOPOLOGY_NAME, name);
        StormTopology topology = buildTopology(numSpout, numBolt, spoutParallelism, boltParallelism);
        TopologyDetails topo = new TopologyDetails(name + "-" + launchTime, conf, topology,
                3, genExecsAndComps(topology, spoutParallelism, boltParallelism), launchTime, "user");
        return topo;
    }

    public static Map<ExecutorDetails, String> genExecsAndComps(StormTopology topology, int spoutParallelism, int boltParallelism) {
        Map<ExecutorDetails, String> retMap = new HashMap<>();
        int startTask = 0;
        int endTask = 1;
        for (Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
            for (int i = 0; i < spoutParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), entry.getKey());
                startTask++;
                endTask++;
            }
        }

        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            for (int i = 0; i < boltParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), entry.getKey());
                startTask++;
                endTask++;
            }
        }
        return retMap;
    }

    public static Map<String, SchedulerAssignmentImpl> assignmentMapToImpl(Map<String, SchedulerAssignment> assignmentMap) {
        Map<String, SchedulerAssignmentImpl> impl = new HashMap<>();
        for (Map.Entry<String, SchedulerAssignment> entry : assignmentMap.entrySet()) {
            impl.put(entry.getKey(), (SchedulerAssignmentImpl) entry.getValue());
        }
        return impl;
    }
}
