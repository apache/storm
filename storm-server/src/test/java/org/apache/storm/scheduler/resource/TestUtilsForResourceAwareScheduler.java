/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestUtilsForResourceAwareScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtilsForResourceAwareScheduler.class);
    public static final int currentTime = 1450418597;
    
    public static class TestUserResources {
      private final String name;
      private final Map<String, Number> resources = new HashMap<>();
      
      public TestUserResources(String name, double cpu, double mem) {
        this.name = name;
        resources.put("cpu", cpu);
        resources.put("memory", mem);
      }
      
      public void addSelfTo(Map<String, Map<String, Number>> fullPool) {
        if (fullPool.put(name, resources) != null) {
          throw new IllegalStateException("Cannot have 2 copies of " + name + " in a pool");
        }
      }
    }
    
    public static TestUserResources userRes(String name, double cpu, double mem) {
      return new TestUserResources(name, cpu, mem);
    }
    
    public static Map<String, Double> toDouble(Map<String, Number> resources) {
      Map<String, Double> ret = new HashMap<>();
      for (Entry<String, Number> entry: resources.entrySet()) {
        ret.put(entry.getKey(), entry.getValue().doubleValue());
      }
      return ret;
    }
    
    public static Map<String, Map<String, Number>> userResourcePool(TestUserResources... resources) {
      Map<String, Map<String, Number>> ret = new HashMap<>();
      for (TestUserResources res: resources) {
        res.addSelfTo(ret);
      }
      return ret;
    }
    
    public static Config createClusterConfig(double compPcore, double compOnHeap, double compOffHeap, Map<String, Map<String, Number>> pools) {
      Config config = new Config();
      config.putAll(Utils.readDefaultConfig());
      config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, DefaultEvictionStrategy.class.getName());
      config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());
      config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, DefaultResourceAwareStrategy.class.getName());
      config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, compPcore);
      config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, compOffHeap);
      config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, compOnHeap);
      if (pools != null) {
        config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS, pools);
      }
      return config;
    }

    public static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts, double cpu, double mem) {
      return genSupervisors(numSup, numPorts, 0, cpu, mem);
    }
    
    public static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts, int start, double cpu, double mem) {
      Map<String, Double> resourceMap = new HashMap<>();
      resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, cpu);
      resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, mem);
        Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
        for (int i = start; i < numSup + start; i++) {
            List<Number> ports = new LinkedList<Number>();
            for (int j = 0; j < numPorts; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, resourceMap);
            retList.put(sup.getId(), sup);
        }
        return retList;
    }

    public static Map<ExecutorDetails, String> genExecsAndComps(StormTopology topology) {
        Map<ExecutorDetails, String> retMap = new HashMap<ExecutorDetails, String>();
        int startTask = 0;
        int endTask = 0;
        for (Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
            SpoutSpec spout = entry.getValue();
            String spoutId = entry.getKey();
            int spoutParallelism = spout.get_common().get_parallelism_hint();
            for (int i = 0; i < spoutParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), spoutId);
                startTask++;
                endTask++;
            }
        }

        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String boltId = entry.getKey();
            Bolt bolt = entry.getValue();
            int boltParallelism = bolt.get_common().get_parallelism_hint();
            for (int i = 0; i < boltParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), boltId);
                startTask++;
                endTask++;
            }
        }
        return retMap;
    }

    public static Topologies addTopologies(Topologies topos, TopologyDetails ... details) {
      Map<String, TopologyDetails> topoMap = new HashMap<>();
      for (TopologyDetails td: topos.getTopologies()) {
        topoMap.put(td.getId(), td);
      }
      for (TopologyDetails td: details) {
        if (topoMap.put(td.getId(), td) != null) {
          throw new IllegalArgumentException("Cannot have multiple topologies with id " + td.getId());
        }
      }
      return new Topologies(topoMap);
    }

    public static TopologyDetails genTopology(String name, Map<String, Object> config, int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism, int launchTime, int priority,
                                              String user) {

        Config conf = new Config();
        conf.putAll(config);
        conf.put(Config.TOPOLOGY_PRIORITY, priority);
        conf.put(Config.TOPOLOGY_NAME, name);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        StormTopology topology = buildTopology(numSpout, numBolt, spoutParallelism, boltParallelism);
        TopologyDetails topo = new TopologyDetails(name + "-" + launchTime, conf, topology,
                0,
                genExecsAndComps(topology), launchTime, user);
        return topo;
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
            j++;
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

        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        public void close() {
        }

        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            _collector.emit(new Values(word));
        }

        public void ack(Object msgId) {
        }

        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if (!_isDistributed) {
                Map<String, Object> ret = new HashMap<String, Object>();
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
        public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {

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

    private static boolean isContain(String source, String subItem) {
        String pattern = "\\b" + subItem + "\\b";
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(source);
        return m.find();
    }

    public static void assertTopologiesNotScheduled(Cluster cluster, String ... topoNames) {
      Topologies topologies = cluster.getTopologies();
      for (String topoName: topoNames) {
        TopologyDetails td = topologies.getByName(topoName);
        assert(td != null) : topoName;
        String topoId = td.getId();
        String status = cluster.getStatus(topoId);
        assert(status != null) : topoName;
        assert(!isStatusSuccess(status)) : topoName;
        assert(cluster.getAssignmentById(topoId) == null) : topoName;
        assert(cluster.needsSchedulingRas(td)) : topoName;
      }
    }

    public static void assertTopologiesFullyScheduled(Cluster cluster, String ... topoNames) {
      Topologies topologies = cluster.getTopologies();
      for (String topoName: topoNames) {
        TopologyDetails td = topologies.getByName(topoName);
        assert(td != null) : topoName;
        String topoId = td.getId();
        assertStatusSuccess(cluster, topoId);
        assert(cluster.getAssignmentById(topoId) != null) : topoName;
        assert(cluster.needsSchedulingRas(td) == false) : topoName;
      }
    }
    
    public static void assertStatusSuccess(Cluster cluster, String topoId) {
      assert(isStatusSuccess(cluster.getStatus(topoId))) : "topology status " + topoId;
    }
      
    public static boolean isStatusSuccess(String status) {
        return isContain(status, "fully") && isContain(status, "scheduled") && !isContain(status, "unsuccessful");
    }

    public static TopologyDetails findTopologyInSetFromName(String topoName, Set<TopologyDetails> set) {
        TopologyDetails ret = null;
        for (TopologyDetails entry : set) {
            if (entry.getName().equals(topoName)) {
                ret = entry;
            }
        }
        return ret;
    }

    public static Map<SupervisorDetails, Double> getSupervisorToMemoryUsage(ISchedulingState cluster, Topologies topologies) {
        Map<SupervisorDetails, Double> superToMem = new HashMap<>();
        Collection<SchedulerAssignment> assignments = cluster.getAssignments().values();
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        for (SupervisorDetails supervisor : supervisors) {
            superToMem.put(supervisor, 0.0);
        }

        for (SchedulerAssignment assignment : assignments) {
            Map<ExecutorDetails, SupervisorDetails> executorToSupervisor = new HashMap<>();
            Map<SupervisorDetails, List<ExecutorDetails>> supervisorToExecutors = new HashMap<>();
            TopologyDetails topology = topologies.getById(assignment.getTopologyId());
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : assignment.getExecutorToSlot().entrySet()) {
                executorToSupervisor.put(entry.getKey(), cluster.getSupervisorById(entry.getValue().getNodeId()));
            }
            for (Map.Entry<ExecutorDetails, SupervisorDetails> entry : executorToSupervisor.entrySet()) {
                List<ExecutorDetails> executorsOnSupervisor = supervisorToExecutors.get(entry.getValue());
                if (executorsOnSupervisor == null) {
                    executorsOnSupervisor = new ArrayList<>();
                    supervisorToExecutors.put(entry.getValue(), executorsOnSupervisor);
                }
                executorsOnSupervisor.add(entry.getKey());
            }
            for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : supervisorToExecutors.entrySet()) {
                Double supervisorUsedMemory = 0.0;
                for (ExecutorDetails executor: entry.getValue()) {
                    supervisorUsedMemory += topology.getTotalMemReqTask(executor);
                }
                superToMem.put(entry.getKey(), superToMem.get(entry.getKey()) + supervisorUsedMemory);
            }
        }
        return superToMem;
    }

    public static Map<SupervisorDetails, Double> getSupervisorToCpuUsage(ISchedulingState cluster, Topologies topologies) {
        Map<SupervisorDetails, Double> superToCpu = new HashMap<>();
        Collection<SchedulerAssignment> assignments = cluster.getAssignments().values();
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        for (SupervisorDetails supervisor : supervisors) {
            superToCpu.put(supervisor, 0.0);
        }

        for (SchedulerAssignment assignment : assignments) {
            Map<ExecutorDetails, SupervisorDetails> executorToSupervisor = new HashMap<>();
            Map<SupervisorDetails, List<ExecutorDetails>> supervisorToExecutors = new HashMap<>();
            TopologyDetails topology = topologies.getById(assignment.getTopologyId());
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : assignment.getExecutorToSlot().entrySet()) {
                executorToSupervisor.put(entry.getKey(), cluster.getSupervisorById(entry.getValue().getNodeId()));
            }
            for (Map.Entry<ExecutorDetails, SupervisorDetails> entry : executorToSupervisor.entrySet()) {
                List<ExecutorDetails> executorsOnSupervisor = supervisorToExecutors.get(entry.getValue());
                if (executorsOnSupervisor == null) {
                    executorsOnSupervisor = new ArrayList<>();
                    supervisorToExecutors.put(entry.getValue(), executorsOnSupervisor);
                }
                executorsOnSupervisor.add(entry.getKey());
            }
            for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : supervisorToExecutors.entrySet()) {
                Double supervisorUsedCpu = 0.0;
                for (ExecutorDetails executor: entry.getValue()) {
                    supervisorUsedCpu += topology.getTotalCpuReqTask(executor);
                }
                superToCpu.put(entry.getKey(), superToCpu.get(entry.getKey()) + supervisorUsedCpu);
            }
        }
        return superToCpu;
    }
}
