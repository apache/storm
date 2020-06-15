/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import com.codahale.metrics.Meter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SingleTopologyCluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.scheduler.utils.ConfigLoaderFactoryService;
import org.apache.storm.scheduler.utils.IConfigLoader;
import org.apache.storm.scheduler.utils.SchedulerConfigCache;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.utils.DisallowedStrategyException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceAwareScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceAwareScheduler.class);
    private Map<String, Object> conf;
    private ISchedulingPriorityStrategy schedulingPriorityStrategy;
    private IConfigLoader configLoader;
    private int maxSchedulingAttempts;
    private int schedulingTimeoutSeconds;
    private ExecutorService backgroundScheduling;
    private Map<String, Set<String>> evictedTopologiesMap;   // topoId : toposEvicted
    private Meter schedulingTimeoutMeter;
    private Meter internalErrorMeter;
    private SchedulerConfigCache<Map<String, Map<String, Double>>> schedulerConfigCache;

    private static void markFailedTopology(User u, Cluster c, TopologyDetails td, String message) {
        markFailedTopology(u, c, td, message, null);
    }

    private static void markFailedTopology(User u, Cluster c, TopologyDetails td, String message, Throwable t) {
        c.setStatus(td, message);
        String realMessage = td.getId() + " " + message;
        if (t != null) {
            LOG.error(realMessage, t);
        } else {
            LOG.error(realMessage);
        }
        u.markTopoUnsuccess(td);
    }

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        this.conf = conf;
        schedulingTimeoutMeter = metricsRegistry.registerMeter("nimbus:num-scheduling-timeouts");
        internalErrorMeter = metricsRegistry.registerMeter("nimbus:scheduler-internal-errors");
        schedulingPriorityStrategy = ReflectionUtils.newInstance(
            (String) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));
        configLoader = ConfigLoaderFactoryService.createConfigLoader(conf);
        maxSchedulingAttempts = ObjectReader.getInt(
            conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_TOPOLOGY_SCHEDULING_ATTEMPTS), 5);
        schedulingTimeoutSeconds = ObjectReader.getInt(
                conf.get(DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY), 60);
        backgroundScheduling = Executors.newFixedThreadPool(1);
        evictedTopologiesMap = new HashMap<>();

        schedulerConfigCache = new SchedulerConfigCache<>(conf, this::loadConfig);
        schedulerConfigCache.prepare();
    }

    @Override
    public void cleanup() {
        LOG.info("Cleanup ResourceAwareScheduler scheduler");
        backgroundScheduling.shutdown();
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return Collections.unmodifiableMap(schedulerConfigCache.get());
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        //refresh the config every time before scheduling
        schedulerConfigCache.refresh();

        Map<String, User> userMap = getUsers(cluster);
        List<TopologyDetails> orderedTopologies = new ArrayList<>(schedulingPriorityStrategy.getOrderedTopologies(cluster, userMap));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ordered list of topologies is: {}", orderedTopologies.stream().map((t) -> t.getId()).collect(Collectors.toList()));
        }
        // clear tmpEvictedTopologiesMap at the beginning of each round of scheduling
        // move it to evictedTopologiesMap at the end of this round of scheduling
        Map<String, Set<String>> tmpEvictedTopologiesMap = new HashMap<>();
        for (TopologyDetails td : orderedTopologies) {
            if (!cluster.needsSchedulingRas(td)) {
                //cluster forgets about its previous status, so if it is scheduled just leave it.
                cluster.setStatusIfAbsent(td.getId(), "Fully Scheduled");
            } else {
                User submitter = userMap.get(td.getTopologySubmitter());
                scheduleTopology(td, cluster, submitter, orderedTopologies, tmpEvictedTopologiesMap);
            }
        }
        evictedTopologiesMap = tmpEvictedTopologiesMap;
    }

    private void scheduleTopology(TopologyDetails td, Cluster cluster, final User topologySubmitter,
                                  List<TopologyDetails> orderedTopologies, Map<String, Set<String>> tmpEvictedTopologiesMap) {
        //A copy of cluster that we can modify, but does not get committed back to cluster unless scheduling succeeds
        Cluster workingState = new Cluster(cluster);
        RasNodes nodes = new RasNodes(workingState);
        IStrategy rasStrategy = null;
        String strategyConf = (String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY);
        try {
            String strategy = (String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY);
            if (strategy.startsWith("backtype.storm")) {
                // Storm support to launch workers of older version.
                // If the config of TOPOLOGY_SCHEDULER_STRATEGY comes from the older version, replace the package name.
                strategy = strategy.replace("backtype.storm", "org.apache.storm");
                LOG.debug("Replaced backtype.storm with org.apache.storm for Config.TOPOLOGY_SCHEDULER_STRATEGY");
            }
            rasStrategy = ReflectionUtils.newSchedulerStrategyInstance(strategy, conf);
            rasStrategy.prepare(conf);
        } catch (DisallowedStrategyException e) {
            markFailedTopology(topologySubmitter, cluster, td,
                               "Unsuccessful in scheduling - " + e.getAttemptedClass()
                               + " is not an allowed strategy. Please make sure your "
                               + Config.TOPOLOGY_SCHEDULER_STRATEGY
                               + " config is one of the allowed strategies: "
                               + e.getAllowedStrategies(), e);
            return;
        } catch (RuntimeException e) {
            markFailedTopology(topologySubmitter, cluster, td,
                               "Unsuccessful in scheduling - failed to create instance of topology strategy "
                               + strategyConf
                               + ". Please check logs for details", e);
            return;
        }

        // Log warning here to avoid duplicating / spamming in strategy / scheduling code.
        boolean oneExecutorPerWorker = (Boolean) td.getConf().get(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER);
        boolean oneComponentPerWorker = (Boolean) td.getConf().get(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER);
        if (oneExecutorPerWorker && oneComponentPerWorker) {
            LOG.warn("Conflicting options: {} and {} are both set! Ignoring {} option.",
                Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER,
                Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER);
        }

        TopologySchedulingResources topologySchedulingResources = new TopologySchedulingResources(workingState, td);
        final IStrategy finalRasStrategy = rasStrategy;
        for (int i = 0; i < maxSchedulingAttempts; i++) {
            SingleTopologyCluster toSchedule = new SingleTopologyCluster(workingState, td.getId());
            try {
                SchedulingResult result = null;
                topologySchedulingResources.resetRemaining();
                if (topologySchedulingResources.canSchedule()) {
                    Future<SchedulingResult> schedulingFuture = backgroundScheduling.submit(
                        () -> finalRasStrategy.schedule(toSchedule, td));
                    try {
                        result = schedulingFuture.get(schedulingTimeoutSeconds, TimeUnit.SECONDS);
                    } catch (TimeoutException te) {
                        markFailedTopology(topologySubmitter, cluster, td, "Scheduling took too long for "
                                + td.getId() + " using strategy " + rasStrategy.getClass().getName() + " timeout after "
                                + schedulingTimeoutSeconds + " seconds using config "
                                + DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY + ".");
                        schedulingTimeoutMeter.mark();
                        schedulingFuture.cancel(true);
                        return;
                    }
                } else {
                    result = SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "");
                }
                LOG.debug("scheduling result: {}", result);
                if (result == null) {
                    markFailedTopology(topologySubmitter, cluster, td, "Internal scheduler error");
                    return;
                } else {
                    if (result.isSuccess()) {
                        cluster.updateFrom(toSchedule);
                        cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                        //DONE
                        return;
                    } else if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                        LOG.debug("Not enough resources to schedule {}", td.getName());
                        List<TopologyDetails> reversedList = ImmutableList.copyOf(orderedTopologies).reverse();
                        LOG.debug("Attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
                        int tdIndex = reversedList.indexOf(td);
                        topologySchedulingResources.setRemainingRequiredResources(toSchedule, td);

                        Set<String> tmpEvictedTopos = new HashSet<>();
                        for (int index = 0; index < tdIndex; index++) {
                            TopologyDetails topologyEvict = reversedList.get(index);
                            SchedulerAssignment evictAssignemnt = workingState.getAssignmentById(topologyEvict.getId());
                            if (evictAssignemnt != null && !evictAssignemnt.getSlots().isEmpty()) {
                                topologySchedulingResources.adjustResourcesForEvictedTopology(toSchedule, topologyEvict);
                                tmpEvictedTopos.add(topologyEvict.getId());
                                Collection<WorkerSlot> workersToEvict = workingState.getUsedSlotsByTopologyId(topologyEvict.getId());
                                nodes.freeSlots(workersToEvict);
                                if (topologySchedulingResources.canSchedule()) {
                                    //We evicted enough topologies to have a hope of scheduling, so try it now, and don't evict more
                                    // than is needed
                                    break;
                                }
                            }
                        }
                        if (!tmpEvictedTopos.isEmpty()) {
                            LOG.warn("Evicted Topologies {} when scheduling topology: {}", tmpEvictedTopos, td.getId());
                            tmpEvictedTopologiesMap.computeIfAbsent(td.getId(), k -> new HashSet<>()).addAll(tmpEvictedTopos);
                        } else {
                            StringBuilder message = new StringBuilder();
                            message.append("Not enough resources to schedule after evicting lower priority topologies. ");
                            message.append(topologySchedulingResources.getRemainingRequiredResourcesMessage());
                            message.append(result.getErrorMessage());
                            markFailedTopology(topologySubmitter, cluster, td, message.toString());
                            return;
                        }
                        //Only place we fall though to do the loop over again...
                    } else { //Any other failure result
                        topologySubmitter.markTopoUnsuccess(td, cluster, result.toString());
                        return;
                    }
                }
            } catch (Exception ex) {
                internalErrorMeter.mark();
                markFailedTopology(topologySubmitter, cluster, td,
                        "Internal Error - Exception thrown when scheduling. Please check logs for details", ex);
                return;
            }
        }
        // We can only reach here when we failed to free enough space by evicting current topologies after {maxSchedulingAttempts}
        // while that scheduler did evict something at each attempt.
        markFailedTopology(topologySubmitter, cluster, td,
            "Failed to make enough resources for " + td.getId()
                    + " by evicting lower priority topologies within " + maxSchedulingAttempts + " attempts. "
                    + topologySchedulingResources.getRemainingRequiredResourcesMessage());
    }

    /**
     * Return eviction information as map {scheduled topo : evicted topos}
     * NOTE this method returns the map of a completed scheduling round.
     * If scheduling is going on, this method will return a map of last scheduling round
     * <p>
     * TODO: This method is only used for testing . It's subject to change if we plan to use this info elsewhere.
     * </p>
     * @return a MAP of scheduled (topo : evicted) topos of most recent completed scheduling round
     */
    public Map<String, Set<String>> getEvictedTopologiesMap() {
        return Collections.unmodifiableMap(evictedTopologiesMap);
    }


    /*
     * Class for tracking resources for scheduling a topology.
     *
     * Ideally we would simply track NormalizedResources, but shared topology memory complicates things.
     * Topologies with shared memory may use more than the SharedMemoryLowerBound, and topologyRequiredResources
     * ignores shared memory.
     *
     * Resources are tracked in two ways:
     * 1) AvailableResources. Track cluster available resources and required topology resources.
     * 2) RemainingRequiredResources. Start with required topology resources, and deduct for partially scheduled and evicted topologies.
     */
    private class TopologySchedulingResources {
        boolean remainingResourcesAreSet;

        NormalizedResourceOffer clusterAvailableResources;
        NormalizedResourceRequest topologyRequiredResources;
        NormalizedResourceRequest topologyScheduledResources;

        double clusterAvailableMemory;
        double topologyRequiredNonSharedMemory;
        double topologySharedMemoryLowerBound;

        NormalizedResourceOffer remainingRequiredTopologyResources;
        double remainingRequiredTopologyMemory;
        double topologyScheduledMemory;

        TopologySchedulingResources(Cluster cluster, TopologyDetails td) {
            remainingResourcesAreSet = false;

            // available resources (lower bound since blacklisted supervisors do not contribute)
            clusterAvailableResources = cluster.getNonBlacklistedClusterAvailableResources(Collections.emptyList());
            clusterAvailableMemory = clusterAvailableResources.getTotalMemoryMb();
            // required resources
            topologyRequiredResources = td.getApproximateTotalResources();
            topologyRequiredNonSharedMemory = td.getRequestedNonSharedOffHeap() + td.getRequestedNonSharedOnHeap();
            topologySharedMemoryLowerBound = td.getRequestedSharedOffHeap() + td.getRequestedSharedOnHeap();
            // partially scheduled topology resources
            setScheduledTopologyResources(cluster, td);
        }

        void setScheduledTopologyResources(Cluster cluster, TopologyDetails td) {
            SchedulerAssignment assignment = cluster.getAssignmentById(td.getId());
            if (assignment != null) {
                topologyScheduledResources = td.getApproximateResources(assignment.getExecutors());
                topologyScheduledMemory = computeScheduledTopologyMemory(cluster, td);
            } else {
                topologyScheduledResources = new NormalizedResourceRequest();
                topologyScheduledMemory = 0;
            }
        }

        boolean canSchedule() {
            return canScheduleAvailable() && canScheduleRemainingRequired();
        }

        boolean canScheduleAvailable() {
            NormalizedResourceOffer availableResources = new NormalizedResourceOffer(clusterAvailableResources);
            availableResources.add(topologyScheduledResources);
            boolean insufficientResources = availableResources.remove(topologyRequiredResources);
            if (insufficientResources) {
                return false;
            }

            double availableMemory = clusterAvailableMemory + topologyScheduledMemory;
            double totalRequiredTopologyMemory = topologyRequiredNonSharedMemory + topologySharedMemoryLowerBound;
            return (availableMemory >= totalRequiredTopologyMemory);
        }

        boolean canScheduleRemainingRequired() {
            if (!remainingResourcesAreSet) {
                return true;
            }
            if (remainingRequiredTopologyResources.areAnyOverZero() || (remainingRequiredTopologyMemory > 0)) {
                return false;
            }

            return true;
        }

        // Set remainingRequiredResources following failed scheduling.
        void setRemainingRequiredResources(Cluster cluster, TopologyDetails td) {
            remainingResourcesAreSet = true;
            setScheduledTopologyResources(cluster, td);

            remainingRequiredTopologyResources = new NormalizedResourceOffer();
            remainingRequiredTopologyResources.add(topologyRequiredResources);
            remainingRequiredTopologyResources.remove(topologyScheduledResources);

            remainingRequiredTopologyMemory = (topologyRequiredNonSharedMemory + topologySharedMemoryLowerBound)
                    - (topologyScheduledMemory);
        }

        // Adjust remainingRequiredResources after evicting topology
        void adjustResourcesForEvictedTopology(Cluster cluster, TopologyDetails evict) {
            SchedulerAssignment assignment = cluster.getAssignmentById(evict.getId());
            if (assignment != null) {
                NormalizedResourceRequest evictResources = evict.getApproximateResources(assignment.getExecutors());
                double topologyScheduledMemory = computeScheduledTopologyMemory(cluster, evict);

                clusterAvailableResources.add(evictResources);
                clusterAvailableMemory += topologyScheduledMemory;
                remainingRequiredTopologyResources.remove(evictResources);
                remainingRequiredTopologyMemory -= topologyScheduledMemory;
            }
        }

        void resetRemaining() {
            remainingResourcesAreSet = false;
            remainingRequiredTopologyMemory = 0;
        }

        private double getMemoryUsed(SchedulerAssignment assignment) {
            return assignment.getScheduledResources().values().stream()
                    .mapToDouble((wr) -> wr.get_mem_on_heap() + wr.get_mem_off_heap()).sum();
        }

        // Get total memory for scheduled topology, including all shared memory
        private double computeScheduledTopologyMemory(Cluster cluster, TopologyDetails td) {
            SchedulerAssignment assignment = cluster.getAssignmentById(td.getId());
            double scheduledTopologyMemory = 0;
            // node shared memory
            if (assignment != null) {
                for (double mem : assignment.getNodeIdToTotalSharedOffHeapNodeMemory().values()) {
                    scheduledTopologyMemory += mem;
                }
                // worker memory (shared & unshared)
                scheduledTopologyMemory += getMemoryUsed(assignment);
            }

            return scheduledTopologyMemory;
        }

        String getRemainingRequiredResourcesMessage() {
            StringBuilder message = new StringBuilder();

            NormalizedResourceOffer clusterRemainingAvailableResources = new NormalizedResourceOffer();
            clusterRemainingAvailableResources.add(clusterAvailableResources);
            clusterRemainingAvailableResources.remove(topologyScheduledResources);

            double memoryNeeded = remainingRequiredTopologyMemory;
            double cpuNeeded = remainingRequiredTopologyResources.getTotalCpu();
            if (memoryNeeded > 0) {
                message.append("Additional Memory Required: ").append(memoryNeeded).append(" MB ");
                message.append("(Available: ").append(clusterRemainingAvailableResources.getTotalMemoryMb()).append(" MB). ");
            }
            if (cpuNeeded > 0) {
                message.append("Additional CPU Required: ").append(cpuNeeded).append("% CPU ");
                message.append("(Available: ").append(clusterRemainingAvailableResources.getTotalCpu()).append(" % CPU).");
            }
            if (remainingRequiredTopologyResources.getNormalizedResources().anyNonCpuOverZero()) {
                message.append(" Additional Topology Required Resources: ");
                message.append(remainingRequiredTopologyResources.getNormalizedResources().toString());
                message.append(" Cluster Available Resources: ");
                message.append(clusterRemainingAvailableResources.getNormalizedResources().toString());
                message.append(".  ");
            }
            return message.toString();
        }
    }

    /**
     * Get User wrappers around cluster.
     *
     * @param cluster the cluster to get the users out of.
     */
    private Map<String, User> getUsers(Cluster cluster) {
        Map<String, User> userMap = new HashMap<>();
        Map<String, Map<String, Double>> userResourcePools = config();
        LOG.debug("userResourcePools: {}", userResourcePools);

        for (TopologyDetails td : cluster.getTopologies()) {
            String topologySubmitter = td.getTopologySubmitter();
            //additional safety check to make sure that topologySubmitter is going to be a valid value
            if (topologySubmitter == null || topologySubmitter.equals("")) {
                LOG.error("Cannot determine user for topology {}.  Will skip scheduling this topology", td.getName());
                continue;
            }
            if (!userMap.containsKey(topologySubmitter)) {
                userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
        }
        return userMap;
    }

    private Map<String, Map<String, Double>> convertToDouble(Map<String, Map<String, Number>> raw) {
        Map<String, Map<String, Double>> ret = new HashMap<>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : raw.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        return ret;
    }

    /**
     * Get resource guarantee configs.
     * Load from configLoaders first; if no config available, read from user-resource-pools.yaml;
     * if no config available from user-resource-pools.yaml, get configs from conf. Only one will be used.
     * @return a map that contains resource guarantees of every user of the following format
     *     {userid->{resourceType->amountGuaranteed}}
     */
    private Map<String, Map<String, Double>> loadConfig() {
        Map<String, Map<String, Number>> raw;

        // Try the loader plugin, if configured
        if (configLoader != null) {
            raw = (Map<String, Map<String, Number>>) configLoader.load(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
            if (raw != null) {
                return convertToDouble(raw);
            } else {
                LOG.warn("Config loader returned null. Will try to read from user-resource-pools.yaml");
            }
        }

        // if no configs from loader, try to read from user-resource-pools.yaml
        Map<String, Object> fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        raw = (Map<String, Map<String, Number>>) fromFile.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (raw != null) {
            return convertToDouble(raw);
        } else {
            LOG.warn("Reading from user-resource-pools.yaml returned null. This could because the file is not available. "
                     + "Will load configs from storm configuration");
        }

        // if no configs from user-resource-pools.yaml, get configs from conf
        raw = (Map<String, Map<String, Number>>) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);

        return convertToDouble(raw);
    }
}
