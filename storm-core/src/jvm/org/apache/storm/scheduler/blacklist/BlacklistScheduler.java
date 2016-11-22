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
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;
import org.apache.storm.scheduler.blacklist.strategies.IBlacklistStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class BlacklistScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(BlacklistScheduler.class);
    IScheduler underlyingScheduler;
    @SuppressWarnings("rawtypes")
    private Map _conf;

    private int toleranceTime;
    private int toleranceCount;
    private int resumeTime;
    private IReporter reporter;
    private IBlacklistStrategy blacklistStrategy;

    private int nimbusMonitorFreqSecs;

    private Map<String, Set<Integer>> cachedSupervisors;

    //key is supervisor key ,value is supervisor ports
    private CircularBuffer<HashMap<String, Set<Integer>>> badSupervisorsTolerance;
    private Set<String> blacklistHost;

    public BlacklistScheduler(IScheduler underlyingScheduler) {
        this.underlyingScheduler = underlyingScheduler;
    }

    @Override
    public void prepare(Map conf) {
        LOG.info("prepare black list scheduler");
        LOG.info(conf.toString());
        underlyingScheduler.prepare(conf);
        _conf = conf;
        if (_conf.containsKey(Config.BLACKLIST_SCHEDULER_TOLERANCE_TIME)) {
            toleranceTime = (Integer) _conf.get(Config.BLACKLIST_SCHEDULER_TOLERANCE_TIME);
        }
        if (_conf.containsKey(Config.BLACKLIST_SCHEDULER_TOLERANCE_COUNT)) {
            toleranceCount = (Integer) _conf.get(Config.BLACKLIST_SCHEDULER_TOLERANCE_COUNT);
        }
        if (_conf.containsKey(Config.BLACKLIST_SCHEDULER_RESUME_TIME)) {
            resumeTime = (Integer) _conf.get(Config.BLACKLIST_SCHEDULER_RESUME_TIME);
        }
        String reporterClassName = _conf.containsKey(Config.BLACKLIST_SCHEDULER_REPORTER) ? (String) _conf.get(Config.BLACKLIST_SCHEDULER_REPORTER) : "";
        try {
            reporter = (IReporter) Class.forName(reporterClassName).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find blacklist reporter for name {}", reporterClassName);
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            LOG.error("Throw InstantiationException blacklist reporter for name {}", reporterClassName);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error("Throw illegalAccessException blacklist reporter for name {}", reporterClassName);
            throw new RuntimeException(e);
        }

        String strategyClassName = _conf.containsKey(Config.BLACKLIST_SCHEDULER_STRATEGY) ? (String) _conf.get(Config.BLACKLIST_SCHEDULER_STRATEGY) : "";
        try {
            blacklistStrategy = (IBlacklistStrategy) Class.forName(strategyClassName).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find blacklist strategy for name {}", strategyClassName);
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            LOG.error("Throw InstantiationException blacklist strategy for name {}", strategyClassName);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error("Throw illegalAccessException blacklist strategy for name {}", strategyClassName);
            throw new RuntimeException(e);
        }

        nimbusMonitorFreqSecs = (Integer) _conf.get(Config.NIMBUS_MONITOR_FREQ_SECS);
        blacklistStrategy.prepare(reporter, toleranceTime, toleranceCount, resumeTime, nimbusMonitorFreqSecs);

        badSupervisorsTolerance = new CircularBuffer<HashMap<String, Set<Integer>>>(toleranceTime / nimbusMonitorFreqSecs);
        cachedSupervisors = new HashMap<>();
        blacklistHost = new HashSet<>();

        StormMetricsRegistry.registerGauge("nimbus:num-blacklisted-supervisor", new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                //nimbus:num-blacklisted-supervisor + none blacklisted supervisor = nimbus:num-supervisors
                return blacklistHost.size();
            }
        });
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("running Black List scheduler");
        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            SupervisorDetails supervisorDetails = entry.getValue();
            String hosts = supervisorDetails.getHost();
            Set<Integer> ports = supervisorDetails.getAllPorts();
            LOG.debug("supervisor: {}, ports: {}", hosts, ports);
        }
        LOG.debug("AssignableSlots: {}", cluster.getAssignableSlots());
        LOG.debug("AvailableSlots: {}", cluster.getAvailableSlots());
        LOG.debug("UsedSlots: {}", cluster.getUsedSlots());

        blacklistStrategy.resumeFromBlacklist();
        badSupervisors(supervisors);
        cluster.setBlacklistedHosts(getBlacklistHosts(cluster, topologies));
        removeLongTimeDisappearFromCache();

        underlyingScheduler.schedule(topologies, cluster);
    }

    private void badSupervisors(Map<String, SupervisorDetails> supervisors) {
        Set<String> cachedSupervisorsKeySet = cachedSupervisors.keySet();
        Set<String> supervisorsKeySet = supervisors.keySet();

        Set<String> badSupervisorKeys = Sets.difference(cachedSupervisorsKeySet, supervisorsKeySet);//cached supervisor doesn't show up
        HashMap<String, Set<Integer>> badSupervisors = new HashMap<String, Set<Integer>>();
        for (String key : badSupervisorKeys) {
            badSupervisors.put(key, cachedSupervisors.get(key));
        }

        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String key = entry.getKey();
            SupervisorDetails supervisorDetails = entry.getValue();
            if (cachedSupervisors.containsKey(key)) {
                Set<Integer> badSlots = badSlots(supervisors, key);
                if (badSlots.size() > 0) {//supervisor contains bad slots
                    badSupervisors.put(key, badSlots);
                }
            } else {
                cachedSupervisors.put(key, supervisorDetails.getAllPorts());//new supervisor to cache
            }
        }

        badSupervisorsTolerance.add(badSupervisors);
    }

    private Set<Integer> badSlots(Map<String, SupervisorDetails> supervisors, String supervisorKey) {
        SupervisorDetails supervisor = supervisors.get(supervisorKey);
        Set<Integer> cachedSupervisorPorts = cachedSupervisors.get(supervisorKey);
        Set<Integer> supervisorPorts = supervisor.getAllPorts();

        Set<Integer> newPorts = Sets.difference(supervisorPorts, cachedSupervisorPorts);
        if (newPorts.size() > 0) {
            cachedSupervisors.put(supervisorKey, Sets.union(newPorts, supervisor.getAllPorts()));
        }

        Set<Integer> difference = Sets.difference(cachedSupervisorPorts, supervisorPorts);
        Set<Integer> badSlots = new HashSet<>();
        for (int port : difference) {
            badSlots.add(port);
        }
        return badSlots;
    }

    public Set<String> getBlacklistHosts(Cluster cluster, Topologies topologies) {
        Set<String> blacklist = blacklistStrategy.getBlacklist(badSupervisorsTolerance.toList(), cluster, topologies);
        Set<String> blacklistHost = new HashSet<>();
        for (String supervisor : blacklist) {
            String host = cluster.getHost(supervisor);
            if (host != null) {
                blacklistHost.add(host);
            } else {
                LOG.info("supervisor {} is not alive know, do not need to add to blacklist.", supervisor);
            }
        }
        this.blacklistHost = blacklistHost;
        return blacklistHost;
    }

    //supervisor or port never exits once in tolerance time will be removed from cache
    private void removeLongTimeDisappearFromCache() {

        Map<String, Integer> supervisorCountMap = new HashMap<String, Integer>();
        Map<WorkerSlot, Integer> slotCountMap = new HashMap<WorkerSlot, Integer>();

        for (Map<String, Set<Integer>> item : badSupervisorsTolerance) {
            Set<String> supervisors = item.keySet();
            for (String supervisor : supervisors) {
                int supervisorCount = 0;
                if (supervisorCountMap.containsKey(supervisor)) {
                    supervisorCount = supervisorCountMap.get(supervisor);
                }
                supervisorCountMap.put(supervisor, supervisorCount + 1);
                for (Integer slot : item.get(supervisor)) {
                    int slotCount = 0;
                    WorkerSlot workerSlot = new WorkerSlot(supervisor, slot);
                    if (slotCountMap.containsKey(workerSlot)) {
                        slotCount = slotCountMap.get(workerSlot);
                    }
                    slotCountMap.put(workerSlot, slotCount + 1);
                }
            }
        }

        int windowSize = badSupervisorsTolerance.capacity();
        for (Map.Entry<String, Integer> entry : supervisorCountMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            if (value == windowSize) {//supervisor never exits once in tolerance time will be removed from cache
                cachedSupervisors.remove(key);
                LOG.info("supervisor {} has never exited once during tolerance time, proberbly be dead forever, removed from cache.", key);
            }
        }

        for (Map.Entry<WorkerSlot, Integer> entry : slotCountMap.entrySet()) {
            WorkerSlot workerSlot = entry.getKey();
            String supervisorKey = workerSlot.getNodeId();
            Integer slot = workerSlot.getPort();
            int value = entry.getValue();
            if (value == windowSize) {//port never exits once in tolerance time will be removed from cache
                Set<Integer> slots = cachedSupervisors.get(supervisorKey);
                if (slots != null) {//slots will be null while supervisor has been removed from cached supervisors
                    slots.remove(slot);
                }
                cachedSupervisors.put(supervisorKey, slots);
                LOG.info("slot {} has never exited once during tolerance time, proberbly be dead forever, removed from cache.", workerSlot);
            }
        }
    }
}