/*
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

package org.apache.storm.daemon.nimbus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a cache of heartbeats from the workers.
 */
public class HeartbeatCache {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatCache.class);
    private static final Function<String, ConcurrentHashMap<List<Integer>, ExecutorCache>> MAKE_MAP = (k) -> new ConcurrentHashMap<>();

    private static class ExecutorCache {
        private Boolean isTimedOut;
        private Integer nimbusTimeSecs;
        private Integer executorReportedTimeSecs;

        ExecutorCache(Map<String, Object> newBeat) {
            if (newBeat != null) {
                executorReportedTimeSecs = (Integer) newBeat.getOrDefault(ClientStatsUtil.TIME_SECS, 0);
            } else {
                executorReportedTimeSecs = 0;
            }

            nimbusTimeSecs = Time.currentTimeSecs();
            isTimedOut = false;
        }

        public synchronized Boolean isTimedOut() {
            return isTimedOut;
        }

        public synchronized Integer getNimbusTimeSecs() {
            return nimbusTimeSecs;
        }

        public synchronized void updateTimeout(Integer timeout) {
            isTimedOut = Time.deltaSecs(getNimbusTimeSecs()) >= timeout;
        }

        public synchronized void updateFromHb(Integer timeout, Map<String, Object> newBeat) {
            if (newBeat != null) {
                Integer newReportedTime = (Integer) newBeat.getOrDefault(ClientStatsUtil.TIME_SECS, 0);
                if (!newReportedTime.equals(executorReportedTimeSecs)) {
                    nimbusTimeSecs = Time.currentTimeSecs();
                }
                executorReportedTimeSecs = newReportedTime;
            }
            updateTimeout(timeout);
        }
    }

    //Topology Id -> executor ids -> component -> stats(...)
    private final ConcurrentHashMap<String, ConcurrentHashMap<List<Integer>, ExecutorCache>> cache;

    /**
     * Create an empty cache.
     */
    public HeartbeatCache() {
        this.cache = new ConcurrentHashMap<>();
    }

    /**
     * Add an empty topology to the cache for testing purposes.
     * @param topoId the id of the topology to add.
     */
    @VisibleForTesting
    public void addEmptyTopoForTests(String topoId) {
        cache.put(topoId, new ConcurrentHashMap<>());
    }

    /**
     * Get the number of topologies with cached heartbeats.
     * @return the number of topologies with cached heartbeats.
     */
    @VisibleForTesting
    public int getNumToposCached() {
        return cache.size();
    }

    /**
     * Get the topology ids with cached heartbeats.
     * @return the set of topology ids with cached heartbeats.
     */
    @VisibleForTesting
    public Set<String> getTopologyIds() {
        return cache.keySet();
    }

    /**
     * Remove a specific topology from the cache.
     * @param topoId the id of the topology to remove.
     */
    public void removeTopo(String topoId) {
        cache.remove(topoId);
    }

    /**
     * Go through all executors and time them out if needed.
     * @param topoId the id of the topology to look at.
     * @param taskTimeoutSecs the timeout to know if they are too old.
     */
    public void timeoutOldHeartbeats(String topoId, Integer taskTimeoutSecs) {
        Map<List<Integer>, ExecutorCache> topoCache = cache.computeIfAbsent(topoId, MAKE_MAP);
        for (ExecutorCache ec : topoCache.values()) {
            ec.updateTimeout(taskTimeoutSecs);
        }
    }

    /**
     * Update the cache with heartbeats from a worker through zookeeper.
     * @param topoId the id to the topology.
     * @param executorBeats the HB data.
     * @param allExecutors the executors.
     * @param timeout the timeout.
     */
    public void updateFromZkHeartbeat(String topoId, Map<List<Integer>, Map<String, Object>> executorBeats,
                                      Set<List<Integer>> allExecutors, Integer timeout) {
        Map<List<Integer>, ExecutorCache> topoCache = cache.computeIfAbsent(topoId, MAKE_MAP);
        if (executorBeats == null) {
            executorBeats = new HashMap<>();
        }

        for (List<Integer> executor : allExecutors) {
            final Map<String, Object> newBeat = executorBeats.get(executor);
            ExecutorCache currBeat = topoCache.computeIfAbsent(executor, (k) -> new ExecutorCache(newBeat));
            currBeat.updateFromHb(timeout, newBeat);
        }
    }

    /**
     * Update the heartbeats for a given worker.
     * @param workerHeartbeat the heartbeats from the worker.
     * @param taskTimeoutSecs the timeout we should be looking at.
     */
    public void updateHeartbeat(SupervisorWorkerHeartbeat workerHeartbeat, Integer taskTimeoutSecs) {
        Map<List<Integer>, Map<String, Object>> executorBeats = StatsUtil.convertWorkerBeats(workerHeartbeat);
        String topoId = workerHeartbeat.get_storm_id();
        Map<List<Integer>, ExecutorCache> topoCache = cache.computeIfAbsent(topoId, MAKE_MAP);

        for (ExecutorInfo executorInfo : workerHeartbeat.get_executors()) {
            List<Integer> executor = Arrays.asList(executorInfo.get_task_start(), executorInfo.get_task_end());
            final Map<String, Object> newBeat = executorBeats.get(executor);
            ExecutorCache currBeat = topoCache.computeIfAbsent(executor, (k) -> new ExecutorCache(newBeat));
            currBeat.updateFromHb(taskTimeoutSecs, newBeat);
        }
    }

    /**
     * Get all of the alive executors for a given topology.
     * @param topoId the id of the topology we are looking for.
     * @param allExecutors all of the executors for this topology.
     * @param assignment the current topology assignment.
     * @param taskLaunchSecs timeout for right after a worker is launched.
     * @return the set of tasks that are alive.
     */
    public Set<List<Integer>> getAliveExecutors(String topoId, Set<List<Integer>> allExecutors, Assignment assignment, int taskLaunchSecs) {
        Map<List<Integer>, ExecutorCache> topoCache = cache.computeIfAbsent(topoId, MAKE_MAP);
        LOG.debug("Computing alive executors for {}\nExecutors: {}\nAssignment: {}\nHeartbeat cache: {}",
            topoId, allExecutors, assignment, topoCache);

        Set<List<Integer>> ret = new HashSet<>();
        Map<List<Long>, Long> execToStartTimes = assignment.get_executor_start_time_secs();

        for (List<Integer> exec : allExecutors) {
            List<Long> longExec = new ArrayList<>(exec.size());
            for (Integer num : exec) {
                longExec.add(num.longValue());
            }

            Long startTime = execToStartTimes.get(longExec);
            ExecutorCache executorCache = topoCache.get(exec);
            //null isTimedOut means worker never reported any heartbeat
            boolean isTimedOut = executorCache == null ? true : executorCache.isTimedOut();
            Integer delta = startTime == null ? null : Time.deltaSecs(startTime.intValue());
            if (startTime != null && ((delta < taskLaunchSecs) || !isTimedOut)) {
                ret.add(exec);
            } else {
                LOG.info("Executor {}:{} not alive", topoId, exec);
            }
        }
        return ret;
    }
}
