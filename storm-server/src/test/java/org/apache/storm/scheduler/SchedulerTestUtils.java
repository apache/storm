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

package org.apache.storm.scheduler;

import org.apache.logging.log4j.util.Strings;
import org.apache.storm.Config;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.utils.ISchedulingTracer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public class SchedulerTestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerTestUtils.class);

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

    public static StormBase buildStormBase(
        String topoName,
        Map<String, Object> topoConf,
        TopologyStatus initStatus,
        StormTopology stormTopology) throws InvalidTopologyException {

        Map<String, Integer> numExecutors = new HashMap<>();
        StormTopology topology = StormCommon.systemTopology(topoConf, stormTopology);
        for (Map.Entry<String, Object> entry : StormCommon.allComponents(topology).entrySet()) {
            numExecutors.put(entry.getKey(), StormCommon.numStartExecutors(entry.getValue()));
        }

        StormBase base = new StormBase();
        base.set_name(topoName);
        if (topoConf.containsKey(Config.TOPOLOGY_VERSION)) {
            base.set_topology_version(ObjectReader.getString(topoConf.get(Config.TOPOLOGY_VERSION)));
        }
        base.set_launch_time_secs(Time.currentTimeSecs());
        base.set_status(initStatus);
        base.set_num_workers(ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 0));
        base.set_component_executors(numExecutors);
        base.set_owner("benji");
        base.set_principal("nimbus");
        base.set_component_debug(new HashMap<>());
        return base;
    }

    public static StormTopology buildTopology(int numSpout, int numBolt,
                                              int spoutParallelism, int boltParallelism) {
        LOG.debug("buildTopology with -> numSpout: " + numSpout + " spoutParallelism: "
            + spoutParallelism + " numBolt: "
            + numBolt + " boltParallelism: " + boltParallelism);
        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < numSpout; i++) {
            builder.setSpout("spout-" + i, new TestSpout(),
                spoutParallelism);
        }
        int j = 0;
        for (int i = 0; i < numBolt; i++) {
            if (j >= numSpout) {
                j = 0;
            }
            builder.setBolt("bolt-" + i, new TestBolt(),
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
        // Flag to indicate whether the nodeToScheduledResourcesCache, nodeToUsedSlotsCache,
        // totalResourcesPerNodeCache are initialized for scheduling, the initialization work is broadly done by
        // leader master instance.
        private volatile boolean isResourceCacheInitialized = false;

        private final Map<String, Map<WorkerSlot, NormalizedResourceRequest>> nodeToScheduledResourcesCache =
            new HashMap<>();
        private final Map<String, Set<WorkerSlot>> nodeToUsedSlotsCache = new HashMap<>();
        private final Map<String, NormalizedResourceRequest> totalResourcesPerNodeCache = new HashMap<>();

        private static final Function<String, Set<WorkerSlot>> MAKE_SET = (x) -> new HashSet<>();
        private static final Function<String, Map<WorkerSlot, NormalizedResourceRequest>> MAKE_MAP =
            (x) -> new HashMap<>();

        @Override
        public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {

        }

        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(
            Collection<SupervisorDetails> existingSupervisors,
            Topologies topologies,
            Set<String> topologiesMissingAssignments) {
            Set<WorkerSlot> ret = new HashSet<>();
            if (existingSupervisors == null) {
                return null;
            }

            for (SupervisorDetails sd : existingSupervisors) {
                String id = sd.getId();
                for (Number port : (Collection<Number>) sd.getMeta()) {
                    ret.add(new WorkerSlot(id, port));
                }
            }
            return ret;
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

        @Override
        public boolean isResourceCacheInitialized() {
            return isResourceCacheInitialized;
        }

        @Override
        public void setResourceCacheInitialized() {
            isResourceCacheInitialized = true;
        }

        // Only used for testing.
        public void setResourceCacheInitialized(boolean initialized) {
            isResourceCacheInitialized = initialized;
        }

        @Override
        public Map<String, Map<WorkerSlot, NormalizedResourceRequest>> getNodeToScheduledResourcesCache() {
            return nodeToScheduledResourcesCache;
        }

        @Override
        public Map<String, Set<WorkerSlot>> getNodeToUsedSlotsCache() {
            return nodeToUsedSlotsCache;
        }

        @Override
        public Map<String, NormalizedResourceRequest> getTotalResourcesPerNodeCache() {
            return totalResourcesPerNodeCache;
        }

        @Override
        public void freeSlotCache(WorkerSlot slot) {
            nodeToScheduledResourcesCache
                .computeIfAbsent(slot.getNodeId(), MAKE_MAP).put(slot, new NormalizedResourceRequest());
            nodeToUsedSlotsCache.computeIfAbsent(slot.getNodeId(), MAKE_SET).remove(slot);
        }
    }

    public static class TestSchedulingTracer implements ISchedulingTracer {
        private List<SortedMap<Long, String>> tmp = new ArrayList<>();

        private SortedMap<Long, String> points = new TreeMap<>();
        private static final String WHITE_SPACE = " ";
        private static final String TRACE_SEP = "====================================================";

        @Override
        public void trace(String pointName) {
            this.points.put(System.currentTimeMillis(), pointName);
        }

        @Override
        public String printActionTrace() {
            if (points.isEmpty()) {
                return Strings.EMPTY;
            }
            return wrapWithTraceSep(doPrintActionTrace(this.points));
        }

        @Override
        public String printAllActionTrace() {
            if (points.isEmpty()) {
                return Strings.EMPTY;
            }
            // We may expect equals size here, but not with strict check.
            // Preconditions.checkState(tmp.stream().allMatch(pts -> pts.size() == points.size()));
            roll();
            StringBuilder builder = new StringBuilder();
            tmp.forEach(
                pts -> builder.append(wrapWithTraceSep(doPrintActionTrace(pts))));
            return builder.toString();
        }

        private String wrapWithTraceSep(String s) {
            StringBuilder builder = new StringBuilder();
            builder.append(Strings.LINE_SEPARATOR);
            builder.append(TRACE_SEP);
            builder.append(Strings.LINE_SEPARATOR);
            builder.append(s);
            builder.append(TRACE_SEP);
            builder.append(Strings.LINE_SEPARATOR);
            return builder.toString();
        }

        private String doPrintActionTrace(SortedMap<Long, String> points) {
            StringBuilder builder = new StringBuilder();
            if (points.size() == 1) {
                Map.Entry<Long, String> point = points.entrySet().stream().findFirst().get();
                builder.append(point.getValue())
                    .append(WHITE_SPACE)
                    .append(point.getKey())
                    .append("ms");
                return builder.toString();
            }

            Map.Entry<Long, String> pre = null;
            for (Map.Entry<Long, String> entry : points.entrySet()) {
                if (pre == null) {
                    pre = entry;
                    continue;
                }
                String firstColumn = pre.getValue() + "->" + entry.getValue();
                Long secondColumn = entry.getKey() - pre.getKey();

                builder.append(firstColumn)
                    .append(WHITE_SPACE)
                    .append(secondColumn)
                    .append("ms")
                    .append(Strings.LINE_SEPARATOR);
                pre = entry;
            }
            return builder.toString();
        }

        @Override
        public void reset() {
            points.clear();
        }

        @Override
        public void roll() {
            this.tmp.add(new TreeMap<>(this.points));
            reset();
        }
    }
}
