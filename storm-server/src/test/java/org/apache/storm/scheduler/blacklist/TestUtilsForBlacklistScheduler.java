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
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class TestUtilsForBlacklistScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtilsForBlacklistScheduler.class);

    public static Map<String, SupervisorDetails> removeSupervisorFromSupervisors(Map<String, SupervisorDetails> supervisorDetailsMap, String supervisor) {
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        retList.putAll(supervisorDetailsMap);
        retList.remove(supervisor);
        return retList;
    }

    public static Map<String, SupervisorDetails> removePortFromSupervisors(Map<String, SupervisorDetails> supervisorDetailsMap, String supervisor, int port) {
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        for (Map.Entry<String, SupervisorDetails> supervisorDetailsEntry : supervisorDetailsMap.entrySet()) {
            String supervisorKey = supervisorDetailsEntry.getKey();
            SupervisorDetails supervisorDetails = supervisorDetailsEntry.getValue();
            Set<Integer> ports = new HashSet<>();
            ports.addAll(supervisorDetails.getAllPorts());
            if (supervisorKey.equals(supervisor)) {
                ports.remove(port);
            }
            SupervisorDetails sup = new SupervisorDetails(supervisorDetails.getId(), supervisorDetails.getHost(), null, (HashSet) ports, null);
            retList.put(sup.getId(), sup);
        }
        return retList;
    }

    public static Map<String, SupervisorDetails> addPortToSupervisors(Map<String, SupervisorDetails> supervisorDetailsMap, String supervisor, int port) {
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        for (Map.Entry<String, SupervisorDetails> supervisorDetailsEntry : supervisorDetailsMap.entrySet()) {
            String supervisorKey = supervisorDetailsEntry.getKey();
            SupervisorDetails supervisorDetails = supervisorDetailsEntry.getValue();
            Set<Integer> ports = new HashSet<>();
            ports.addAll(supervisorDetails.getAllPorts());
            if (supervisorKey.equals(supervisor)) {
                ports.add(port);
            }
            SupervisorDetails sup = new SupervisorDetails(supervisorDetails.getId(), supervisorDetails.getHost(), null, (HashSet) ports, null);
            retList.put(sup.getId(), sup);
        }
        return retList;
    }

    public static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts) {
        Map<String, SupervisorDetails> retList = new HashMap<>();
        for (int i = 0; i < numSup; i++) {
            List<Number> ports = new LinkedList<>();
            for (int j = 0; j < numPorts; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, null);
            retList.put(sup.getId(), sup);
        }
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

    public static StormTopology buildTopology(int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism) {
        LOG.debug("buildTopology with -> numSpout: " + numSpout + " spoutParallelism: "
                + spoutParallelism + " numBolt: "
                + numBolt + " boltParallelism: " + boltParallelism);
        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < numSpout; i++) {
            SpoutDeclarer s1 = builder.setSpout("spout-" + i, new TestSpout(),
                    spoutParallelism);
        }
        int j = 0;
        for (int i = 0; i < numBolt; i++) {
            if (j >= numSpout) {
                j = 0;
            }
            BoltDeclarer b1 = builder.setBolt("bolt-" + i, new TestBolt(),
                    boltParallelism).shuffleGrouping("spout-" + j);
        }

        return builder.createTopology();
    }

    public static class TestSpout extends BaseRichSpout {
        boolean _isDistributed;
        SpoutOutputCollector _collector;

        public TestSpout() {
            this(true);
        }

        public TestSpout(boolean isDistributed) {
            _isDistributed = isDistributed;
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void close() {
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            _collector.emit(new Values(word));
        }

        @Override
        public void ack(Object msgId) {
        }

        @Override
        public void fail(Object msgId) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if (!_isDistributed) {
                Map<String, Object> ret = new HashMap<>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            } else {
                return null;
            }
        }
    }

    public static class TestBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context,
                            OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class INimbusTest implements INimbus {
        @Override
        public void prepare(Map<String, Object> stormConf, String schedulerLocalDir) {

        }

        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
            return null;
        }

        @Override
        public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {

        }

        @Override
        public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
            if (existingSupervisors.containsKey(nodeId)) {
                return existingSupervisors.get(nodeId).getHost();
            }
            return null;
        }

        @Override
        public IScheduler getForcedScheduler() {
            return null;
        }
    }

    public static Map<String, SchedulerAssignmentImpl> assignmentMapToImpl(Map<String, SchedulerAssignment> assignmentMap) {
        Map<String, SchedulerAssignmentImpl> impl = new HashMap<>();
        for (Map.Entry<String, SchedulerAssignment> entry : assignmentMap.entrySet()) {
            impl.put(entry.getKey(), (SchedulerAssignmentImpl) entry.getValue());
        }
        return impl;
    }
}
