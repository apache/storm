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

package org.apache.storm.scheduler.blacklist.strategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;
import org.apache.storm.scheduler.blacklist.reporters.LogReporter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default strategy used for blacklisting hosts.
 */
public class DefaultBlacklistStrategy implements IBlacklistStrategy {

    public static final int DEFAULT_BLACKLIST_SCHEDULER_RESUME_TIME = 1800;
    public static final int DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_COUNT = 3;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBlacklistStrategy.class);
    private IReporter reporter;

    private int toleranceCount;
    private int resumeTime;
    private int nimbusMonitorFreqSecs;

    private TreeMap<String, Integer> blacklist;

    @Override
    public void prepare(Map<String, Object> conf) {
        toleranceCount = ObjectReader.getInt(conf.get(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT),
                                             DEFAULT_BLACKLIST_SCHEDULER_TOLERANCE_COUNT);
        resumeTime = ObjectReader.getInt(conf.get(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME), DEFAULT_BLACKLIST_SCHEDULER_RESUME_TIME);

        String reporterClassName = ObjectReader.getString(conf.get(DaemonConfig.BLACKLIST_SCHEDULER_REPORTER),
                                                          LogReporter.class.getName());
        reporter = (IReporter) initializeInstance(reporterClassName, "blacklist reporter");

        nimbusMonitorFreqSecs = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS));
        blacklist = new TreeMap<>();
    }

    @Override
    public Set<String> getBlacklist(List<Map<String, Set<Integer>>> supervisorsWithFailures,
                                    List<Map<String, Integer>> sendAssignmentFailureCount,
                                    Cluster cluster, Topologies topologies) {
        Map<String, Integer> countMap = new HashMap<>();

        for (Map<String, Set<Integer>> item : supervisorsWithFailures) {
            Set<String> supervisors = item.keySet();
            for (String supervisor : supervisors) {
                int supervisorCount = countMap.getOrDefault(supervisor, 0);
                countMap.put(supervisor, supervisorCount + 1);
            }
        }

        // update countMap failures for sendAssignments failing
        for (Map<String, Integer> item : sendAssignmentFailureCount) {
            for (Map.Entry<String, Integer> entry : item.entrySet()) {
                String supervisorNode = entry.getKey();
                int sendAssignmentFailures = entry.getValue() + countMap.getOrDefault(supervisorNode, 0);
                countMap.put(supervisorNode, sendAssignmentFailures);
            }
        }

        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            String supervisor = entry.getKey();
            int count = entry.getValue();
            if (count >= toleranceCount) {
                if (!blacklist.containsKey(supervisor)) { // if not in blacklist then add it and set the resume time according to config
                    LOG.debug("Added supervisor {} to blacklist", supervisor);
                    LOG.debug("supervisorsWithFailures : {}", supervisorsWithFailures);
                    LOG.debug("sendAssignmentFailureCount: {}", sendAssignmentFailureCount);
                    reporter.reportBlacklist(supervisor, supervisorsWithFailures);
                    blacklist.put(supervisor, resumeTime / nimbusMonitorFreqSecs);
                }
            }
        }
        Set<String> toRelease = releaseBlacklistWhenNeeded(cluster, new ArrayList<>(blacklist.keySet()));
        // After having computed the final blacklist,
        // the nodes which are released due to resource shortage will be put to the "greylist".
        if (toRelease != null) {
            LOG.debug("Releasing {} nodes because of low resources", toRelease.size());
            cluster.setGreyListedSupervisors(toRelease);
            for (String key : toRelease) {
                blacklist.remove(key);
            }
        }
        return blacklist.keySet();
    }

    @Override
    public void resumeFromBlacklist() {
        Set<String> readyToRemove = new HashSet<>();
        for (Map.Entry<String, Integer> entry : blacklist.entrySet()) {
            String supervisor = entry.getKey();
            int countUntilResume = entry.getValue() - 1;
            if (countUntilResume == 0) {
                readyToRemove.add(supervisor);
            } else {
                blacklist.put(supervisor, countUntilResume);
            }
        }
        for (String key : readyToRemove) {
            blacklist.remove(key);
            LOG.info("Supervisor {} has been blacklisted more than resume period. Removed from blacklist.", key);
        }
    }

    /**
     * Decide when/if to release blacklisted hosts.
     * @param cluster the current state of the cluster.
     * @param blacklistedNodeIds the current set of blacklisted node ids sorted by earliest
     * @return the set of nodes to be released.
     */
    protected Set<String> releaseBlacklistWhenNeeded(Cluster cluster, final List<String> blacklistedNodeIds) {
        Set<String> readyToRemove = new HashSet<>();
        if (blacklistedNodeIds.size() > 0) {
            int availableSlots = cluster.getNonBlacklistedAvailableSlots(blacklistedNodeIds).size();
            int neededSlots = 0;

            for (TopologyDetails td : cluster.needsSchedulingTopologies()) {
                int slots = td.getNumWorkers();
                int assignedSlots = cluster.getAssignedNumWorkers(td);
                int tdSlotsNeeded = slots - assignedSlots;
                neededSlots += tdSlotsNeeded;
            }

            //Now we need to free up some resources...
            Map<String, SupervisorDetails> availableSupervisors = cluster.getSupervisors();
            int shortageSlots = neededSlots - availableSlots;
            LOG.debug("Need {} slots.", neededSlots);
            LOG.debug("Available {} slots.", availableSlots);
            LOG.debug("Shortage {} slots.", shortageSlots);

            if (shortageSlots > 0) {
                LOG.info("Need {} slots more. Releasing some blacklisted nodes to cover it.", shortageSlots);

                //release earliest blacklist - but release all supervisors on a given blacklisted host.
                Map<String, Set<String>> hostToSupervisorIds = createHostToSupervisorMap(blacklistedNodeIds, cluster);
                for (Set<String> supervisorIds : hostToSupervisorIds.values()) {
                    for (String supervisorId : supervisorIds) {
                        SupervisorDetails sd = availableSupervisors.get(supervisorId);
                        if (sd != null) {
                            int sdAvailableSlots = cluster.getAvailablePorts(sd).size();
                            readyToRemove.add(supervisorId);
                            shortageSlots -= sdAvailableSlots;
                            LOG.debug("Releasing {} with {} slots leaving {} slots to go", supervisorId,
                                    sdAvailableSlots, shortageSlots);
                        }
                    }
                    if (shortageSlots <= 0) {
                        // we have enough resources now...
                        break;
                    }
                }
            }
        }
        return readyToRemove;
    }

    private Object initializeInstance(String className, String representation) {
        try {
            return Class.forName(className).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find {} for name {}", representation, className);
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            LOG.error("Throw InstantiationException {} for name {}", representation, className);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error("Throw IllegalAccessException {} for name {}", representation, className);
            throw new RuntimeException(e);
        }
    }

    protected Map<String, Set<String>> createHostToSupervisorMap(final List<String> blacklistedNodeIds, Cluster cluster) {
        Map<String, Set<String>> hostToSupervisorMap = new TreeMap<>();
        for (String supervisorId : blacklistedNodeIds) {
            String hostname = cluster.getHost(supervisorId);
            if (hostname != null) {
                Set<String> supervisorIds = hostToSupervisorMap.get(hostname);
                if (supervisorIds == null) {
                    supervisorIds = new HashSet<>();
                    hostToSupervisorMap.put(hostname, supervisorIds);
                }
                supervisorIds.add(supervisorId);
            }
        }
        return hostToSupervisorMap;
    }
}
