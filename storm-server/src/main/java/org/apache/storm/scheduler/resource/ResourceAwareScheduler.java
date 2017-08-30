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

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SingleTopologyCluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.strategies.eviction.IEvictionStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.scheduler.utils.ConfigLoaderFactoryService;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.DisallowedStrategyException;
import org.apache.storm.scheduler.utils.IConfigLoader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceAwareScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceAwareScheduler.class);
    private Map<String, Object> conf;
    private ISchedulingPriorityStrategy schedulingPrioritystrategy;
    private IEvictionStrategy evictionStrategy;
    private IConfigLoader configLoader;

    @Override
    public void prepare(Map<String, Object> conf) {
        this.conf = conf;
        schedulingPrioritystrategy = (ISchedulingPriorityStrategy) ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));
        evictionStrategy = (IEvictionStrategy) ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY));
        configLoader = ConfigLoaderFactoryService.createConfigLoader(conf);
    }

    @Override
    public Map<String, Object> config() {
        return (Map) getUserResourcePools();
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        //initialize data structures
        for (TopologyDetails td : cluster.getTopologies()) {
            if (!cluster.needsSchedulingRas(td)) {
                //cluster forgets about its previous status, so if it is scheduled just leave it.
                cluster.setStatusIfAbsent(td.getId(), "Fully Scheduled");
            }
        }
        Map<String, User> userMap = getUsers(cluster);

        while (true) {
            TopologyDetails td;
            try {
                //Call scheduling priority strategy
                td = schedulingPrioritystrategy.getNextTopologyToSchedule(cluster, userMap);
            } catch (Exception ex) {
                LOG.error("Exception thrown when running priority strategy {}. No topologies will be scheduled!",
                        schedulingPrioritystrategy.getClass().getName(), ex);
                break;
            }
            if (td == null) {
                break;
            }
            User submitter = userMap.get(td.getTopologySubmitter());
            if (cluster.needsSchedulingRas(td)) {
                scheduleTopology(td, cluster, submitter, userMap);
            } else {
                LOG.warn("Topology {} is already fully scheduled!", td.getName());
                cluster.setStatusIfAbsent(td.getId(), "Fully Scheduled");
            }
        }
    }


    public void scheduleTopology(TopologyDetails td, Cluster cluster, final User topologySubmitter,
                                 Map<String, User> userMap) {
        //A copy of cluster that we can modify, but does not get committed back to cluster unless scheduling succeeds
        Cluster workingState = new Cluster(cluster);
        IStrategy rasStrategy = null;
        String strategyConf = (String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY);
        try {
            rasStrategy = (IStrategy) ReflectionUtils.newSchedulerStrategyInstance((String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), conf);
            rasStrategy.prepare(conf);
        } catch (DisallowedStrategyException e) {
            topologySubmitter.markTopoUnsuccess(td);
            cluster.setStatus(td.getId(), "Unsuccessful in scheduling - " + e.getAttemptedClass()
                    + " is not an allowed strategy. Please make sure your " + Config.TOPOLOGY_SCHEDULER_STRATEGY
                    + " config is one of the allowed strategies: " + e.getAllowedStrategies().toString());
            return;
        } catch (RuntimeException e) {
            LOG.error("failed to create instance of IStrategy: {} Topology {} will not be scheduled.",
                    strategyConf, td.getName(), e);
            topologySubmitter.markTopoUnsuccess(td);
            cluster.setStatus(td.getId(), "Unsuccessful in scheduling - failed to create instance of topology strategy "
                    + strategyConf + ". Please check logs for details");
            return;
        }

        while (true) {
            // A copy of the cluster that restricts the strategy to only modify a single topology
            SingleTopologyCluster toSchedule = new SingleTopologyCluster(workingState, td.getId());
            SchedulingResult result = null;
            try {
                result = rasStrategy.schedule(toSchedule, td);
            } catch (Exception ex) {
                LOG.error("Exception thrown when running strategy {} to schedule topology {}."
                        + " Topology will not be scheduled!", rasStrategy.getClass().getName(), td.getName(), ex);
                topologySubmitter.markTopoUnsuccess(td);
                cluster.setStatus(td.getId(), "Unsuccessful in scheduling - Exception thrown when running strategy {}"
                        + rasStrategy.getClass().getName() + ". Please check logs for details");
            }
            LOG.debug("scheduling result: {}", result);
            if (result != null) {
                if (result.isSuccess()) {
                    try {
                        cluster.updateFrom(toSchedule);
                        cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                    } catch (Exception ex) {
                        LOG.error("Unsuccessful attempting to assign executors to nodes.", ex);
                        topologySubmitter.markTopoUnsuccess(td);
                        cluster.setStatus(td.getId(), "Unsuccessful in scheduling - "
                                + "IllegalStateException thrown when attempting to assign executors to nodes. Please check"
                                + " log for details.");
                    }
                    return;
                } else {
                    if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                        boolean madeSpace = false;
                        try {
                            //need to re prepare since scheduling state might have been restored
                            madeSpace = evictionStrategy.makeSpaceForTopo(td, workingState, userMap);
                        } catch (Exception ex) {
                            LOG.error("Exception thrown when running eviction strategy {} to schedule topology {}."
                                            + " No evictions will be done!", evictionStrategy.getClass().getName(),
                                    td.getName(), ex);
                            topologySubmitter.markTopoUnsuccess(td);
                            return;
                        }
                        if (!madeSpace) {
                            LOG.debug("Could not make space for topo {} will move to attempted", td);
                            topologySubmitter.markTopoUnsuccess(td);
                            cluster.setStatus(td.getId(), "Not enough resources to schedule - "
                                    + result.getErrorMessage());
                            return;
                        }
                        continue;
                    } else {
                        topologySubmitter.markTopoUnsuccess(td, cluster);
                        return;
                    }
                }
            } else {
                LOG.warn("Scheduling results returned from topology {} is not vaild! Topology with be ignored.",
                        td.getName());
                topologySubmitter.markTopoUnsuccess(td, cluster);
                return;
            }
        }
    }

    /**
     * Get User wrappers around cluster.
     *
     * @param cluster the cluster to get the users out of.
     */
    private Map<String, User> getUsers(Cluster cluster) {
        Map<String, User> userMap = new HashMap<>();
        Map<String, Map<String, Double>> userResourcePools = getUserResourcePools();
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
        Map<String, Map<String, Double>> ret = new HashMap<String, Map<String, Double>>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : raw.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
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
     * {userid->{resourceType->amountGuaranteed}}
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {
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
        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
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
