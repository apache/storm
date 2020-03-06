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

package org.apache.storm.scheduler.blacklist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;
import org.apache.storm.scheduler.blacklist.reporters.LogReporter;
import org.apache.storm.scheduler.blacklist.strategies.DefaultBlacklistStrategy;
import org.apache.storm.scheduler.blacklist.strategies.IBlacklistStrategy;
import org.apache.storm.shade.com.google.common.collect.EvictingQueue;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlacklistScheduler implements IScheduler {
    public static final int DEFAULT_BLACKLIST_SCHEDULER_RESUME_TIME = 1800;
    public static final int DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_COUNT = 3;
    public static final int DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_TIME = 300;
    public static final int DEFAULT_BLACKLIST_SCHEDULER_ASSIGNMENT_FAILURE_TOLERANCE_COUNT = 0;
    private static final Logger LOG = LoggerFactory.getLogger(BlacklistScheduler.class);
    private static final int ASSIGNMENT_ROTATION_INTERVAL_SECONDS = 3600;
    private final IScheduler underlyingScheduler;
    private final StormMetricsRegistry metricsRegistry;
    protected int toleranceTime;
    protected int toleranceCount;
    protected int resumeTime;
    protected IReporter reporter;
    protected IBlacklistStrategy blacklistStrategy;
    protected int nimbusMonitorFreqSecs;
    protected Map<String, Set<Integer>> cachedSupervisors;
    //key is supervisor key ,value is supervisor ports
    protected EvictingQueue<HashMap<String, Set<Integer>>> badSupervisorsToleranceSlidingWindow;
    protected int windowSize;
    protected volatile Set<String> blacklistedSupervisorIds;     // supervisor ids
    private boolean blacklistOnBadSlots;
    private Map<String, Object> conf;
    private RotatingMap<String, Integer> failedAssignmentNodeCounts;  // tracks repeated counts for failed assignment calls for nodes
    private long lastAssignmentRotationSecs = Time.currentTimeSecs();
    private int failedAssignmentToleranceCount;

    public BlacklistScheduler(IScheduler underlyingScheduler, StormMetricsRegistry metricsRegistry) {
        this.underlyingScheduler = underlyingScheduler;
        this.metricsRegistry = metricsRegistry;
        this.failedAssignmentNodeCounts = new RotatingMap<>(null);
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        LOG.info("Preparing black list scheduler");
        underlyingScheduler.prepare(conf);
        this.conf = conf;

        toleranceTime = ObjectReader.getInt(this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME),
                                            DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_TIME);
        toleranceCount = ObjectReader.getInt(this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT),
                                             DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_COUNT);
        failedAssignmentToleranceCount = ObjectReader.getInt(this.conf.get(
                DaemonConfig.BLACKLIST_SCHEDULER_ASSIGNMENT_FAILURE_TOLERANCE_COUNT),
                DEFAULT_BLACKLIST_SCHEDULER_ASSIGNMENT_FAILURE_TOLERANCE_COUNT);
        resumeTime = ObjectReader.getInt(this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME),
                                         DEFAULT_BLACKLIST_SCHEDULER_RESUME_TIME);

        String reporterClassName = ObjectReader.getString(this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_REPORTER),
                                                          LogReporter.class.getName());
        reporter = (IReporter) initializeInstance(reporterClassName, "blacklist reporter");

        String strategyClassName = ObjectReader.getString(this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_STRATEGY),
                                                          DefaultBlacklistStrategy.class.getName());
        blacklistStrategy = (IBlacklistStrategy) initializeInstance(strategyClassName, "blacklist strategy");

        nimbusMonitorFreqSecs = ObjectReader.getInt(this.conf.get(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS));
        blacklistStrategy.prepare(this.conf);

        windowSize = toleranceTime / nimbusMonitorFreqSecs;
        badSupervisorsToleranceSlidingWindow = EvictingQueue.create(windowSize);
        cachedSupervisors = new HashMap<>();
        blacklistedSupervisorIds = new HashSet<>();
        blacklistOnBadSlots = ObjectReader.getBoolean(
                this.conf.get(DaemonConfig.BLACKLIST_SCHEDULER_ASSUME_SUPERVISOR_BAD_BASED_ON_BAD_SLOT),
                true);

        //nimbus:num-blacklisted-supervisor + non-blacklisted supervisor = nimbus:num-supervisors
        metricsRegistry.registerGauge("nimbus:num-blacklisted-supervisor", () -> blacklistedSupervisorIds.size());
    }

    @Override
    public void cleanup() {
        LOG.info("Cleanup black list scheduler");
        underlyingScheduler.cleanup();
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("running Black List scheduler");
        LOG.debug("AssignableSlots: {}", cluster.getAssignableSlots());
        LOG.debug("AvailableSlots: {}", cluster.getAvailableSlots());
        LOG.debug("UsedSlots: {}", cluster.getUsedSlots());

        blacklistStrategy.resumeFromBlacklist();

        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
        determineBadSupervisorsFromHeartbeating(supervisors);
        Map<String, Integer> nodeAssignmentFailures = getNodeAssignmentFailures();
        // this step also frees up some bad supervisors to greylist due to resource shortage
        blacklistedSupervisorIds = refreshBlacklistedSupervisorIds(cluster, nodeAssignmentFailures);
        Set<String> blacklistHosts = getBlacklistHosts(cluster, blacklistedSupervisorIds);
        cluster.setBlacklistedHosts(blacklistHosts);
        removeLongTimeDisappearFromCache();

        underlyingScheduler.schedule(topologies, cluster);
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return underlyingScheduler.config();
    }

    private Map<String, Integer> getNodeAssignmentFailures() {
        Map<String, Integer> nodeFailures = new HashMap<>();
        if (failedAssignmentToleranceCount <= 0) {
            return nodeFailures;
        }

        synchronized (failedAssignmentNodeCounts) {
            // rotate out old assignment failures periodically to prevent tracking supervisors that are
            // no longer in rotation or could have been fixed and added back in rotation.
            if (Time.currentTimeSecs() > lastAssignmentRotationSecs + ASSIGNMENT_ROTATION_INTERVAL_SECONDS) {
                failedAssignmentNodeCounts.rotate();
                lastAssignmentRotationSecs = Time.currentTimeSecs();
            }
            Set<String> keys = failedAssignmentNodeCounts.keyset();
            for (String node : keys) {
                int failures = failedAssignmentNodeCounts.getOrDefault(node, 0);
                nodeFailures.put(node, failures);
                if (failures >= failedAssignmentToleranceCount) {
                    // if we're past the tolerance for this node, we're going to blacklist it.  Clear out the failures so
                    // we don't re-add it to the blacklist immediately once the node comes back into rotation.
                    failedAssignmentNodeCounts.remove(node);
                }
            }
        }
        return nodeFailures;
    }

    private void determineBadSupervisorsFromHeartbeating(Map<String, SupervisorDetails> supervisors) {
        Set<String> cachedSupervisorsKeySet = cachedSupervisors.keySet();
        Set<String> supervisorsKeySet = supervisors.keySet();

        Set<String> badSupervisorKeys = Sets.difference(cachedSupervisorsKeySet, supervisorsKeySet); //cached supervisor doesn't show up
        HashMap<String, Set<Integer>> badSupervisors = new HashMap<>();
        for (String key : badSupervisorKeys) {
            badSupervisors.put(key, cachedSupervisors.get(key));
        }
        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String key = entry.getKey();
            SupervisorDetails supervisorDetails = entry.getValue();
            if (cachedSupervisors.containsKey(key)) {
                if (blacklistOnBadSlots) {
                    Set<Integer> badSlots = badSlots(supervisorDetails, key);
                    if (badSlots.size() > 0) { //supervisor contains bad slots
                        badSupervisors.put(key, badSlots);
                    }
                }
            } else {
                cachedSupervisors.put(key, supervisorDetails.getAllPorts()); //new supervisor to cache
            }
        }
        badSupervisorsToleranceSlidingWindow.add(badSupervisors);
    }

    private Set<Integer> badSlots(SupervisorDetails supervisor, String supervisorKey) {
        Set<Integer> cachedSupervisorPorts = cachedSupervisors.get(supervisorKey);
        Set<Integer> supervisorPorts = supervisor.getAllPorts();

        Set<Integer> newPorts = Sets.difference(supervisorPorts, cachedSupervisorPorts);
        if (newPorts.size() > 0) {
            // add new ports to cached supervisor.  We need a modifiable set to allow removing ports later.
            Set<Integer> allPorts = new HashSet<>(newPorts);
            allPorts.addAll(cachedSupervisorPorts);
            cachedSupervisors.put(supervisorKey, allPorts);
        }
        Set<Integer> badSlots = Sets.difference(cachedSupervisorPorts, supervisorPorts);
        return badSlots;
    }

    private Set<String> refreshBlacklistedSupervisorIds(Cluster cluster, Map<String, Integer> nodeAssignmentFailures) {
        Set<String> blacklistedSupervisors = blacklistStrategy.getBlacklist(new ArrayList<>(badSupervisorsToleranceSlidingWindow),
                cluster, nodeAssignmentFailures);
        LOG.info("Supervisors {} are blacklisted.", blacklistedSupervisors);
        return blacklistedSupervisors;
    }

    private Set<String> getBlacklistHosts(Cluster cluster, Set<String> blacklistIds) {
        Set<String> blacklistHostSet = new HashSet<>();
        for (String supervisor : blacklistIds) {
            String host = cluster.getHost(supervisor);
            if (host != null) {
                blacklistHostSet.add(host);
            } else {
                LOG.info("supervisor {} is not alive, do not need to add to blacklist.", supervisor);
            }
        }
        return blacklistHostSet;
    }

    /**
     * supervisor or port never exits once in tolerance time will be removed from cache.
     */
    private void removeLongTimeDisappearFromCache() {

        Map<String, Integer> supervisorCountMap = new HashMap<String, Integer>();
        Map<WorkerSlot, Integer> slotCountMap = new HashMap<WorkerSlot, Integer>();

        for (Map<String, Set<Integer>> item : badSupervisorsToleranceSlidingWindow) {
            Set<String> supervisors = item.keySet();
            for (String supervisor : supervisors) {
                int supervisorCount = supervisorCountMap.getOrDefault(supervisor, 0);
                Set<Integer> slots = item.get(supervisor);
                // treat supervisor as bad only if all of its slots matched the cached supervisor
                if (slots.equals(cachedSupervisors.get(supervisor))) {
                    // track how many times a cached supervisor has been marked bad
                    supervisorCountMap.put(supervisor, supervisorCount + 1);
                }
                // track how many times each supervisor slot has been listed as bad
                for (Integer slot : slots) {
                    WorkerSlot workerSlot = new WorkerSlot(supervisor, slot);
                    int slotCount = slotCountMap.getOrDefault(workerSlot, 0);
                    slotCountMap.put(workerSlot, slotCount + 1);
                }
            }
        }

        for (Map.Entry<String, Integer> entry : supervisorCountMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            if (value == windowSize) { // supervisor which was never back to normal in tolerance period will be removed from cache
                cachedSupervisors.remove(key);
                LOG.info("Supervisor {} was never back to normal during tolerance period, probably dead. Will remove from cache.", key);
            }
        }

        for (Map.Entry<WorkerSlot, Integer> entry : slotCountMap.entrySet()) {
            WorkerSlot workerSlot = entry.getKey();
            String supervisorKey = workerSlot.getNodeId();
            Integer slot = workerSlot.getPort();
            int slotFailures = entry.getValue();
            if (slotFailures == windowSize) { // worker slot which was never back to normal in tolerance period will be removed from cache
                Set<Integer> slots = cachedSupervisors.get(supervisorKey);
                if (slots != null) { // slots will be null while supervisor has been removed from cached supervisors
                    slots.remove(slot);
                    cachedSupervisors.put(supervisorKey, slots);
                }
                LOG.info("Worker slot {} was never back to normal during tolerance period, probably dead. Will be removed from cache.",
                         workerSlot);
            }
        }
    }

    private Object initializeInstance(String className, String representation) {
        try {
            return ReflectionUtils.newInstance(className);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ClassNotFoundException) {
                LOG.error("Can't find {} for name {}", representation, className);
            } else if (cause instanceof InstantiationException) {
                LOG.error("Throw InstantiationException {} for name {}", representation, className);
            } else if (cause instanceof IllegalAccessException) {
                LOG.error("Throw IllegalAccessException {} for name {}", representation, className);
            } else {
                LOG.error("Throw unexpected exception {} {} for name {}", cause, representation, className);
            }

            throw e;
        }
    }

    public Set<String> getBlacklistSupervisorIds() {
        return Collections.unmodifiableSet(blacklistedSupervisorIds);
    }

    @Override
    public void nodeAssignmentSent(String node, boolean successful) {
        if (failedAssignmentToleranceCount <= 0) {
            // no need to track (and cause synchronization) if we're not blacklisting for this
            return;
        }

        synchronized (failedAssignmentNodeCounts) {
            if (successful) {
                failedAssignmentNodeCounts.remove(node);
            } else {
                int failureCount = failedAssignmentNodeCounts.getOrDefault(node, 0) + 1;
                failedAssignmentNodeCounts.put(node, failureCount);
            }
        }
    }
}
