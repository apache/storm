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

package org.apache.storm.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.utils.Time;

/**
 * Stats calculations needed by storm client code.
 */
public class ClientStatsUtil {
    public static final String SPOUT = "spout";
    public static final String BOLT = "bolt";
    static final String EXECUTOR_STATS = "executor-stats";
    static final String UPTIME = "uptime";
    public static final String TIME_SECS = "time-secs";
    public static final ToGlobalStreamIdTransformer TO_GSID = new ToGlobalStreamIdTransformer();
    public static final IdentityTransformer IDENTITY = new IdentityTransformer();

    /**
     * Convert a List&lt;Long&gt; executor to java List&lt;Integer&gt;.
     */
    public static List<Integer> convertExecutor(List<Long> executor) {
        return Lists.newArrayList(executor.get(0).intValue(), executor.get(1).intValue());
    }

    /**
     * Make an map of executors to empty stats, in preparation for doing a heartbeat.
     * @param executors the executors as keys of the map
     * @return and empty map of executors to stats
     */
    public static Map<List<Integer>, ExecutorStats> mkEmptyExecutorZkHbs(Set<List<Long>> executors) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Object executor : executors) {
            List startEnd = (List) executor;
            ret.put(convertExecutor(startEnd), null);
        }
        return ret;
    }

    /**
     * Convert Long Executor Ids in ZkHbs to Integer ones structure to java maps.
     */
    public static Map<List<Integer>, ExecutorStats> convertExecutorZkHbs(Map<List<Long>, ExecutorStats> executorBeats) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Map.Entry<List<Long>, ExecutorStats> entry : executorBeats.entrySet()) {
            ret.put(convertExecutor(entry.getKey()), entry.getValue());
        }
        return ret;
    }

    /**
     * Create a new worker heartbeat for zookeeper.
     * @param topoId the topology id
     * @param executorStats the stats for the executors
     * @param uptime the uptime for the worker
     * @return the heartbeat map
     */
    public static Map<String, Object> mkZkWorkerHb(String topoId, Map<List<Integer>, ExecutorStats> executorStats, Integer uptime) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("storm-id", topoId);
        ret.put(EXECUTOR_STATS, executorStats);
        ret.put(UPTIME, uptime);
        ret.put(TIME_SECS, Time.currentTimeSecs());

        return ret;
    }

    private static Number getByKeyOr0(Map<String, Object> m, String k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    /**
     * Get a sub-map by a given key.
     * @param map the original map
     * @param key the key to get it from
     * @return the map stored under key
     */
    public static <K, V> Map<K, V> getMapByKey(Map map, String key) {
        if (map == null) {
            return null;
        }
        return (Map<K, V>) map.get(key);
    }

    public static ClusterWorkerHeartbeat thriftifyZkWorkerHb(Map<String, Object> heartbeat) {
        ClusterWorkerHeartbeat ret = new ClusterWorkerHeartbeat();
        ret.set_uptime_secs(getByKeyOr0(heartbeat, UPTIME).intValue());
        ret.set_storm_id((String) heartbeat.get("storm-id"));
        ret.set_time_secs(getByKeyOr0(heartbeat, TIME_SECS).intValue());

        Map<ExecutorInfo, ExecutorStats> convertedStats = new HashMap<>();

        Map<List<Integer>, ExecutorStats> executorStats = getMapByKey(heartbeat, EXECUTOR_STATS);
        if (executorStats != null) {
            for (Map.Entry<List<Integer>, ExecutorStats> entry : executorStats.entrySet()) {
                List<Integer> executor = entry.getKey();
                ExecutorStats stats = entry.getValue();
                if (null != stats) {
                    convertedStats.put(new ExecutorInfo(executor.get(0), executor.get(1)), stats);
                }
            }
        }
        ret.set_executor_stats(convertedStats);

        return ret;
    }

    /**
     * Converts stats to be over given windows of time.
     * @param stats the stats
     * @param secKeyFunc transform the sub-key
     * @param firstKeyFunc transform the main key
     */
    public static <K1, K2> Map windowSetConverter(
        Map stats, KeyTransformer<K2> secKeyFunc, KeyTransformer<K1> firstKeyFunc) {
        Map ret = new HashMap();

        for (Object o : stats.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            K1 key1 = firstKeyFunc.transform(entry.getKey());

            Map subRetMap = (Map) ret.get(key1);
            if (subRetMap == null) {
                subRetMap = new HashMap();
            }
            ret.put(key1, subRetMap);

            Map value = (Map) entry.getValue();
            for (Object oo : value.entrySet()) {
                Map.Entry subEntry = (Map.Entry) oo;
                K2 key2 = secKeyFunc.transform(subEntry.getKey());
                subRetMap.put(key2, subEntry.getValue());
            }
        }
        return ret;
    }

    // =====================================================================================
    // key transformers
    // =====================================================================================

    /**
     * Provides a way to transform one key into another.
     */
    interface KeyTransformer<T> {
        T transform(Object key);
    }

    static class ToGlobalStreamIdTransformer implements KeyTransformer<GlobalStreamId> {
        @Override
        public GlobalStreamId transform(Object key) {
            if (key instanceof List) {
                List l = (List) key;
                if (l.size() > 1) {
                    return new GlobalStreamId((String) l.get(0), (String) l.get(1));
                }
            }
            return new GlobalStreamId("", key.toString());
        }
    }

    static class IdentityTransformer implements KeyTransformer<Object> {
        @Override
        public Object transform(Object key) {
            return key;
        }
    }
}
