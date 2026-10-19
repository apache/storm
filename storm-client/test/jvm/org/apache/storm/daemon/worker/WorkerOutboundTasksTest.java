/*
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

package org.apache.storm.daemon.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WorkerState#computeOutboundTasks}, which decides the set of task ids a worker must be able to
 * route messages to. The key behaviour under test: when upstream feedback is enabled, the upstream (source) component
 * tasks of locally-hosted components must be included, because feedback tuples are routed directly to them. Otherwise
 * they are missing from {@code cachedTaskToNodePort}, the feedback is dropped and {@code TransferDrainer} logs the
 * spurious "No remote destination available for task" warning.
 */
public class WorkerOutboundTasksTest {

    // Linear topology: spout -> bolt1 -> bolt2. Task ids are topology-global and unique per component.
    private static final List<Integer> SPOUT_TASKS = Arrays.asList(1, 2);
    private static final List<Integer> BOLT1_TASKS = Arrays.asList(3, 4);
    private static final List<Integer> BOLT2_TASKS = Arrays.asList(5, 6);
    private static final List<Integer> ACKER_TASKS = Arrays.asList(7);

    private GeneralTopologyContext context() throws InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new NoopSpout());
        builder.setBolt("bolt1", new NoopBolt()).shuffleGrouping("spout");
        builder.setBolt("bolt2", new NoopBolt()).shuffleGrouping("bolt1");
        Map<String, Object> topoConf = Utils.readStormConfig();
        topoConf.put(Config.TOPOLOGY_WORKERS, 1);
        topoConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, ACKER_TASKS.size());
        StormTopology topology = StormCommon.systemTopology(topoConf, builder.createTopology());

        Map<Integer, String> taskToComponent = new HashMap<>();
        SPOUT_TASKS.forEach(t -> taskToComponent.put(t, "spout"));
        BOLT1_TASKS.forEach(t -> taskToComponent.put(t, "bolt1"));
        BOLT2_TASKS.forEach(t -> taskToComponent.put(t, "bolt2"));
        ACKER_TASKS.forEach(t -> taskToComponent.put(t, Acker.ACKER_COMPONENT_ID));

        Map<String, List<Integer>> componentToSortedTasks = new HashMap<>();
        componentToSortedTasks.put("spout", SPOUT_TASKS);
        componentToSortedTasks.put("bolt1", BOLT1_TASKS);
        componentToSortedTasks.put("bolt2", BOLT2_TASKS);
        componentToSortedTasks.put(Acker.ACKER_COMPONENT_ID, ACKER_TASKS);

        GeneralTopologyContext context = new GeneralTopologyContext(topology, topoConf, taskToComponent, componentToSortedTasks,
                new HashMap<>(), "topology-id");
        assertTrue(context.getComponentIds().contains(Acker.ACKER_COMPONENT_ID));
        return context;
    }

    @Test
    public void feedbackDisabled_onlyDownstreamTargets() throws InvalidTopologyException {
        // Worker hosts a bolt1 task. Its only downstream target is bolt2, so the upstream spout tasks are not routable.
        GeneralTopologyContext context = context();
        Set<Integer> outbound = WorkerState.computeOutboundTasks(context, BOLT1_TASKS.subList(0, 1),
                context.getTaskToComponent(), false);

        BOLT2_TASKS.forEach(t -> assertTrue(outbound.contains(t), "downstream bolt2 task " + t + " must remain routable"));
        ACKER_TASKS.forEach(t -> assertTrue(outbound.contains(t), "acker task " + t + " must remain routable"));
        SPOUT_TASKS.forEach(t -> assertFalse(outbound.contains(t), "spout task " + t + " must not be routable without feedback"));
    }

    @Test
    public void feedbackEnabled_includesUpstreamSourceTasks() throws InvalidTopologyException {
        // Worker hosts a bolt1 task. With feedback on, bolt1's upstream spout tasks must also be routable so feedback
        // tuples reach them instead of being dropped.
        GeneralTopologyContext context = context();
        Set<Integer> outbound = WorkerState.computeOutboundTasks(context, BOLT1_TASKS.subList(0, 1),
                context.getTaskToComponent(), true);

        BOLT2_TASKS.forEach(t -> assertTrue(outbound.contains(t), "downstream bolt2 task " + t + " must remain routable"));
        ACKER_TASKS.forEach(t -> assertTrue(outbound.contains(t), "acker task " + t + " must remain routable"));
        SPOUT_TASKS.forEach(t -> assertTrue(outbound.contains(t), "upstream spout task " + t + " must be routable with feedback"));
    }

    @Test
    public void feedbackDisabled_colocatedSpoutMasksMissingUpstreamRouting() throws InvalidTopologyException {
        GeneralTopologyContext context = context();
        List<Integer> localTasks = Arrays.asList(SPOUT_TASKS.get(0), BOLT2_TASKS.get(0));
        Set<Integer> withoutFeedback = WorkerState.computeOutboundTasks(context, localTasks,
                context.getTaskToComponent(), false);
        Set<Integer> withFeedback = WorkerState.computeOutboundTasks(context, localTasks,
                context.getTaskToComponent(), true);

        // The spout already makes bolt1 routable for bolt2 feedback.
        // This demonstrates how the issue with feedback delivery setup could be hidden in particular configurations.
        BOLT1_TASKS.forEach(t -> assertTrue(withoutFeedback.contains(t),
                "bolt1 task " + t + " must already be routable as the colocated spout's downstream target"));
        BOLT1_TASKS.forEach(t -> assertTrue(withFeedback.contains(t),
                "upstream bolt1 task " + t + " must remain routable with feedback"));
        ACKER_TASKS.forEach(t -> assertTrue(withoutFeedback.contains(t), "acker task " + t + " must remain routable"));
        ACKER_TASKS.forEach(t -> assertTrue(withFeedback.contains(t), "acker task " + t + " must remain routable with feedback"));
    }

    @Test
    public void feedbackEnabled_systemComponentsUpstreamNotAddedAsFeedbackTasks() throws InvalidTopologyException {
        GeneralTopologyContext context = context();
        Set<Integer> outbound = WorkerState.computeOutboundTasks(context, ACKER_TASKS,
                context.getTaskToComponent(), true);

        SPOUT_TASKS.forEach(t -> assertTrue(outbound.contains(t), "spout task " + t + " must remain routable for ack/fail messages"));
        BOLT1_TASKS.forEach(t -> assertFalse(outbound.contains(t), "bolt1 task " + t + " must not be routable"));
        BOLT2_TASKS.forEach(t -> assertFalse(outbound.contains(t), "bolt2 task " + t + " must not be routable"));
        ACKER_TASKS.forEach(t -> assertFalse(outbound.contains(t), "acker task " + t + " must not be routable from itself"));
    }

    private static class NoopSpout extends BaseRichSpout implements IRichSpout {
        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        }

        @Override
        public void nextTuple() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    private static class NoopBolt extends BaseRichBolt implements IRichBolt {
        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context,
                            org.apache.storm.task.OutputCollector collector) {
        }

        @Override
        public void execute(Tuple input) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }
}
