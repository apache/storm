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

package org.apache.storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private static final int MAX_WEIGHT = 100;
    private static final Logger LOG = LoggerFactory.getLogger(LoadAwareShuffleGrouping.class);
    private final Map<Integer, IndexAndWeights> orig = new HashMap<>();
    @VisibleForTesting
    List<Integer>[] rets;
    @VisibleForTesting
    volatile int[] choices;
    private int capacity;
    private Random random;
    private volatile int[] prepareChoices;
    private AtomicInteger current;
    private LocalityScope currentScope;
    private NodeInfo sourceNodeInfo;
    private List<Integer> targetTasks;
    private AtomicReference<Map<Integer, NodeInfo>> taskToNodePort;
    private AtomicReference<Map<String, String>> nodeToHost;
    private Map<String, Object> conf;
    private DNSToSwitchMapping dnsToSwitchMapping;
    private Map<LocalityScope, List<Integer>> localityGroup;
    private double higherBound;
    private double lowerBound;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        sourceNodeInfo = new NodeInfo(context.getAssignmentId(), Sets.newHashSet((long) context.getThisWorkerPort()));
        taskToNodePort = context.getTaskToNodePort();
        nodeToHost = context.getNodeToHost();
        this.targetTasks = targetTasks;
        capacity = targetTasks.size() == 1 ? 1 : Math.max(1000, targetTasks.size() * 5);
        conf = context.getConf();
        dnsToSwitchMapping = ReflectionUtils.newInstance((String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN));
        localityGroup = new HashMap<>();
        currentScope = LocalityScope.WORKER_LOCAL;
        higherBound = ObjectReader.getDouble(conf.get(Config.TOPOLOGY_LOCALITYAWARE_HIGHER_BOUND));
        lowerBound = ObjectReader.getDouble(conf.get(Config.TOPOLOGY_LOCALITYAWARE_LOWER_BOUND));

        rets = (List<Integer>[]) new List<?>[targetTasks.size()];
        int i = 0;
        for (int target : targetTasks) {
            rets[i] = Arrays.asList(target);
            orig.put(target, new IndexAndWeights(i));
            i++;
        }

        // can't leave choices to be empty, so initiate it similar as ShuffleGrouping
        choices = new int[capacity];

        current = new AtomicInteger(0);
        // allocate another array to be switched
        prepareChoices = new int[capacity];
        updateRing(null);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        int rightNow;
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < capacity) {
                return rets[choices[rightNow]];
            } else if (rightNow == capacity) {
                current.set(0);
                return rets[choices[0]];
            }
            //race condition with another thread, and we lost
            // try again
        }
    }

    @Override
    public void refreshLoad(LoadMapping loadMapping) {
        updateRing(loadMapping);
    }

    private void refreshLocalityGroup() {
        // taskToNodePort and nodeToHost might be out of sync when they are refreshed by WorkerState
        // but this is okay since it will only cause a temporary misjudgement on LocalityScope
        Map<Integer, NodeInfo> cachedTaskToNodePort = taskToNodePort.get();
        Map<String, String> cachedNodeToHost = nodeToHost.get();

        Map<String, String> hostToRack = getHostToRackMapping(cachedTaskToNodePort, cachedNodeToHost);

        localityGroup.values().stream().forEach(v -> v.clear());

        for (int target : targetTasks) {
            LocalityScope scope = calculateScope(cachedTaskToNodePort, cachedNodeToHost, hostToRack, target);
            LOG.debug("targetTask {} is in LocalityScope {}", target, scope);
            if (!localityGroup.containsKey(scope)) {
                localityGroup.put(scope, new ArrayList<>());
            }
            localityGroup.get(scope).add(target);
        }
    }

    private List<Integer> getTargetsInScope(LocalityScope scope) {
        List<Integer> rets = new ArrayList<>();
        List<Integer> targetInScope = localityGroup.get(scope);
        if (null != targetInScope) {
            rets.addAll(targetInScope);
        }
        LocalityScope downgradeScope = LocalityScope.downgrade(scope);
        if (downgradeScope != scope) {
            rets.addAll(getTargetsInScope(downgradeScope));
        }
        return rets;
    }

    private LocalityScope transition(LoadMapping load) {
        List<Integer> targetInScope = getTargetsInScope(currentScope);
        if (targetInScope.isEmpty()) {
            LocalityScope upScope = LocalityScope.upgrade(currentScope);
            if (upScope == currentScope) {
                throw new RuntimeException("The current scope " + currentScope + " has no target tasks.");
            }
            currentScope = upScope;
            return transition(load);
        }

        if (null == load) {
            return currentScope;
        }

        double avg = targetInScope.stream().mapToDouble((key) -> load.get(key)).average().getAsDouble();

        LocalityScope nextScope = currentScope;
        if (avg > higherBound) {
            nextScope = LocalityScope.upgrade(currentScope);
        } else {
            LocalityScope lowerScope = LocalityScope.downgrade(currentScope);
            List<Integer> lowerTargets = getTargetsInScope(lowerScope);
            if (!lowerTargets.isEmpty()) {
                double lowerAvg = lowerTargets.stream().mapToDouble((key) -> load.get(key)).average().getAsDouble();
                if (lowerAvg < lowerBound) {
                    nextScope = lowerScope;
                }
            }
        }

        return nextScope;
    }

    private synchronized void updateRing(LoadMapping load) {
        refreshLocalityGroup();
        LocalityScope prevScope = currentScope;
        currentScope = transition(load);
        if (currentScope != prevScope) {
            //reset all the weights
            orig.values().stream().forEach(o -> o.resetWeight());
        }

        List<Integer> targetsInScope = getTargetsInScope(currentScope);

        //We will adjust weights based off of the minimum load
        double min = load == null ? 0 : targetsInScope.stream().mapToDouble((key) -> load.get(key)).min().getAsDouble();
        for (int target : targetsInScope) {
            IndexAndWeights val = orig.get(target);
            double l = load == null ? 0.0 : load.get(target);
            if (l <= min + (0.05)) {
                //We assume that within 5% of the minimum congestion is still fine.
                //Not congested we grow (but slowly)
                val.weight = Math.min(MAX_WEIGHT, val.weight + 1);
            } else {
                //Congested we contract much more quickly
                val.weight = Math.max(0, val.weight - 10);
            }
        }
        //Now we need to build the array
        long weightSum = targetsInScope.stream().mapToLong((target) -> orig.get(target).weight).sum();
        //Now we can calculate a percentage

        int currentIdx = 0;
        if (weightSum > 0) {
            for (int target : targetsInScope) {
                IndexAndWeights indexAndWeights = orig.get(target);
                int count = (int) ((indexAndWeights.weight / (double) weightSum) * capacity);
                for (int i = 0; i < count && currentIdx < capacity; i++) {
                    prepareChoices[currentIdx] = indexAndWeights.index;
                    currentIdx++;
                }
            }

            if (currentIdx > 0) {
                //in case we didn't fill in enough
                for (; currentIdx < capacity; currentIdx++) {
                    prepareChoices[currentIdx] = prepareChoices[random.nextInt(currentIdx)];
                }
            }
        }
        if (currentIdx == 0) {
            //This really should be impossible, because we go off of the min load, and inc anything within 5% of it.
            // But just to be sure it is never an issue, especially with float rounding etc.
            for (; currentIdx < capacity; currentIdx++) {
                prepareChoices[currentIdx] = currentIdx % rets.length;
            }
        }

        shuffleArray(prepareChoices);

        // swapping two arrays
        int[] tempForSwap = choices;
        choices = prepareChoices;
        prepareChoices = tempForSwap;

        current.set(-1);
    }

    private void shuffleArray(int[] arr) {
        int size = arr.length;
        for (int i = size; i > 1; i--) {
            swap(arr, i - 1, random.nextInt(i));
        }
    }

    private void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private LocalityScope calculateScope(Map<Integer, NodeInfo> taskToNodePort, Map<String, String> nodeToHost,
                                         Map<String, String> hostToRack, int target) {
        NodeInfo targetNodeInfo = taskToNodePort.get(target);

        if (targetNodeInfo == null) {
            return LocalityScope.EVERYTHING;
        }

        if (sourceNodeInfo.get_node().equals(targetNodeInfo.get_node())) {
            if (sourceNodeInfo.get_port().equals(targetNodeInfo.get_port())) {
                return LocalityScope.WORKER_LOCAL;
            }
            return LocalityScope.HOST_LOCAL;
        } else {
            String sourceHostname = nodeToHost.get(sourceNodeInfo.get_node());
            String targetHostname = nodeToHost.get(targetNodeInfo.get_node());

            String sourceRack = (sourceHostname == null) ? null : hostToRack.get(sourceHostname);
            String targetRack = (targetHostname == null) ? null : hostToRack.get(targetHostname);

            if (sourceRack != null && sourceRack.equals(targetRack)) {
                return LocalityScope.RACK_LOCAL;
            } else {
                return LocalityScope.EVERYTHING;
            }
        }
    }

    private Map<String, String> getHostToRackMapping(Map<Integer, NodeInfo> taskToNodePort, Map<String, String> nodeToHost) {
        Set<String> hosts = new HashSet<>();
        for (int task : targetTasks) {
            //if this task containing worker will be killed by a assignments sync,
            //taskToNodePort will be an empty map which is refreshed by WorkerState
            if (taskToNodePort.containsKey(task)) {
                String node = taskToNodePort.get(task).get_node();
                String hostname = nodeToHost.get(node);
                if (hostname != null) {
                    hosts.add(hostname);
                }
            } else {
                LOG.error("Could not find task NodeInfo from local cache.");
            }
        }

        String node = sourceNodeInfo.get_node();
        String hostname = nodeToHost.get(node);
        if (hostname != null) {
            hosts.add(hostname);
        }
        return dnsToSwitchMapping.resolve(new ArrayList<>(hosts));
    }

    //only for test
    public int getCapacity() {
        return capacity;
    }

    @VisibleForTesting
    public LocalityScope getCurrentScope() {
        return currentScope;
    }

    enum LocalityScope {
        WORKER_LOCAL, HOST_LOCAL, RACK_LOCAL, EVERYTHING;

        public static LocalityScope downgrade(LocalityScope current) {
            switch (current) {
                case EVERYTHING:
                    return RACK_LOCAL;
                case RACK_LOCAL:
                    return HOST_LOCAL;
                case HOST_LOCAL:
                case WORKER_LOCAL:
                default:
                    return WORKER_LOCAL;
            }
        }

        public static LocalityScope upgrade(LocalityScope current) {
            switch (current) {
                case WORKER_LOCAL:
                    return HOST_LOCAL;
                case HOST_LOCAL:
                    return RACK_LOCAL;
                case RACK_LOCAL:
                case EVERYTHING:
                default:
                    return EVERYTHING;
            }
        }
    }

    private static class IndexAndWeights {
        final int index;
        int weight;

        IndexAndWeights(int index) {
            this.index = index;
            weight = MAX_WEIGHT;
        }

        void resetWeight() {
            weight = MAX_WEIGHT;
        }
    }
}