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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.storm.cluster.ExecutorBeat;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StatsUtil {
    public static final String TYPE = "type";
    public static final int TEN_MIN_IN_SECONDS = 60 * 10;
    public static final String TEN_MIN_IN_SECONDS_STR = TEN_MIN_IN_SECONDS + "";
    private static final Logger LOG = LoggerFactory.getLogger(StatsUtil.class);
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String NUM_TASKS = "num-tasks";
    private static final String NUM_EXECUTORS = "num-executors";
    private static final String CAPACITY = "capacity";
    private static final String STATS = "stats";
    private static final String EXECUTOR_ID = "executor-id";
    private static final String LAST_ERROR = "lastError";
    private static final String RATE = "rate";
    private static final String ACKED = "acked";
    private static final String FAILED = "failed";
    private static final String EXECUTED = "executed";
    private static final String EMITTED = "emitted";
    private static final String TRANSFERRED = "transferred";
    private static final String EXEC_LATENCIES = "execute-latencies";
    private static final String PROC_LATENCIES = "process-latencies";
    private static final String COMP_LATENCIES = "complete-latencies";
    private static final String EXEC_LATENCY = "execute-latency";
    private static final String PROC_LATENCY = "process-latency";
    private static final String COMP_LATENCY = "complete-latency";
    private static final String EXEC_LAT_TOTAL = "executeLatencyTotal";
    private static final String PROC_LAT_TOTAL = "processLatencyTotal";
    private static final String COMP_LAT_TOTAL = "completeLatencyTotal";
    private static final String WIN_TO_EMITTED = "window->emitted";
    private static final String WIN_TO_ACKED = "window->acked";
    private static final String WIN_TO_FAILED = "window->failed";
    private static final String WIN_TO_EXECUTED = "window->executed";
    private static final String WIN_TO_TRANSFERRED = "window->transferred";
    private static final String WIN_TO_EXEC_LAT = "window->execute-latency";
    private static final String WIN_TO_PROC_LAT = "window->process-latency";
    private static final String WIN_TO_COMP_LAT = "window->complete-latency";
    private static final String WIN_TO_COMP_LAT_WGT_AVG = "window->comp-lat-wgt-avg";
    private static final String WIN_TO_EXEC_LAT_WGT_AVG = "window->exec-lat-wgt-avg";
    private static final String WIN_TO_PROC_LAT_WGT_AVG = "window->proc-lat-wgt-avg";
    private static final String BOLT_TO_STATS = "bolt-id->stats";
    private static final String SPOUT_TO_STATS = "spout-id->stats";
    private static final String SID_TO_OUT_STATS = "sid->output-stats";
    private static final String CID_SID_TO_IN_STATS = "cid+sid->input-stats";
    private static final String WORKERS_SET = "workers-set";
    private static final ToStringTransformer TO_STRING = new ToStringTransformer();
    private static final FromGlobalStreamIdTransformer FROM_GSID = new FromGlobalStreamIdTransformer();


    // =====================================================================================
    // aggregation stats methods
    // =====================================================================================

    /**
     * Aggregates number executed, process latency, and execute latency across all streams.
     *
     * @param id2execAvg { global stream id -> exec avg value }, e.g., {["split" "default"] 0.44313}
     * @param id2procAvg { global stream id -> proc avg value }
     * @param id2numExec { global stream id -> executed }
     */
    public static Map<String, Number> aggBoltLatAndCount(Map<List<String>, Double> id2execAvg,
                                                         Map<List<String>, Double> id2procAvg,
                                                         Map<List<String>, Long> id2numExec) {
        Map<String, Number> ret = new HashMap<>();
        ((Map) ret).put(EXEC_LAT_TOTAL, weightAvgAndSum(id2execAvg, id2numExec));
        ((Map) ret).put(PROC_LAT_TOTAL, weightAvgAndSum(id2procAvg, id2numExec));
        ((Map) ret).put(EXECUTED, sumValues(id2numExec));

        return ret;
    }

    /**
     * aggregate number acked and complete latencies across all streams.
     */
    public static Map<String, Number> aggSpoutLatAndCount(Map<String, Double> id2compAvg,
                                                          Map<String, Long> id2numAcked) {
        Map<String, Number> ret = new HashMap<>();
        ((Map) ret).put(COMP_LAT_TOTAL, weightAvgAndSum(id2compAvg, id2numAcked));
        ((Map) ret).put(ACKED, sumValues(id2numAcked));

        return ret;
    }

    /**
     * aggregate number executed and process & execute latencies.
     */
    public static <K> Map<K, Map> aggBoltStreamsLatAndCount(Map<K, Double> id2execAvg,
                                                            Map<K, Double> id2procAvg,
                                                            Map<K, Long> id2numExec) {
        Map<K, Map> ret = new HashMap<>();
        if (id2execAvg == null || id2procAvg == null || id2numExec == null) {
            return ret;
        }
        for (K k : id2execAvg.keySet()) {
            Map<String, Object> subMap = new HashMap<>();
            subMap.put(EXEC_LAT_TOTAL, weightAvg(id2execAvg, id2numExec, k));
            subMap.put(PROC_LAT_TOTAL, weightAvg(id2procAvg, id2numExec, k));
            subMap.put(EXECUTED, id2numExec.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    /**
     * Aggregates number acked and complete latencies.
     */
    public static <K> Map<K, Map> aggSpoutStreamsLatAndCount(Map<K, Double> id2compAvg,
                                                             Map<K, Long> id2acked) {
        Map<K, Map> ret = new HashMap<>();
        if (id2compAvg == null || id2acked == null) {
            return ret;
        }
        for (K k : id2compAvg.keySet()) {
            Map subMap = new HashMap();
            subMap.put(COMP_LAT_TOTAL, weightAvg(id2compAvg, id2acked, k));
            subMap.put(ACKED, id2acked.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    /**
     * pre-merge component page bolt stats from an executor heartbeat 1. computes component capacity 2. converts map keys of stats 3.
     * filters streams if necessary
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return per-merged stats
     */
    public static Map<String, Object> aggPreMergeCompPageBolt(Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(EXECUTOR_ID, beat.get("exec-id"));
        ret.put(HOST, beat.get(HOST));
        ret.put(PORT, beat.get(PORT));
        ret.put(ClientStatsUtil.UPTIME, beat.get(ClientStatsUtil.UPTIME));
        ret.put(NUM_EXECUTORS, 1);
        ret.put(NUM_TASKS, beat.get(NUM_TASKS));

        Map stat2win2sid2num = ClientStatsUtil.getMapByKey(beat, STATS);
        ret.put(CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeyOr0(beat, ClientStatsUtil.UPTIME).intValue()));

        // calc cid+sid->input_stats
        Map inputStats = new HashMap();
        Map sid2acked = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, ACKED), TO_STRING).get(window);
        Map sid2failed = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, FAILED), TO_STRING).get(window);
        Object v1 = sid2acked != null ? sid2acked : new HashMap();
        inputStats.put(ACKED, v1);
        Object v = sid2failed != null ? sid2failed : new HashMap();
        inputStats.put(FAILED, v);

        inputStats = swapMapOrder(inputStats);

        Map sid2execLat = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EXEC_LATENCIES), TO_STRING).get(window);
        Map sid2procLat = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, PROC_LATENCIES), TO_STRING).get(window);
        Map sid2exec = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EXECUTED), TO_STRING).get(window);
        mergeMaps(inputStats, aggBoltStreamsLatAndCount(sid2execLat, sid2procLat, sid2exec));
        ret.put(CID_SID_TO_IN_STATS, inputStats);

        // calc sid->output_stats
        Map outputStats = new HashMap();
        Map sid2emitted = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EMITTED), TO_STRING).get(window);
        Map sid2transferred = (Map) windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, TRANSFERRED), TO_STRING).get(window);
        if (sid2emitted != null) {
            outputStats.put(EMITTED, filterSysStreams2Stat(sid2emitted, includeSys));
        } else {
            outputStats.put(EMITTED, new HashMap());
        }
        if (sid2transferred != null) {
            outputStats.put(TRANSFERRED, filterSysStreams2Stat(sid2transferred, includeSys));
        } else {
            outputStats.put(TRANSFERRED, new HashMap());
        }
        outputStats = swapMapOrder(outputStats);
        ret.put(SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    /**
     * pre-merge component page spout stats from an executor heartbeat 1. computes component capacity 2. converts map keys of stats 3.
     * filters streams if necessary
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return per-merged stats
     */
    public static Map<String, Object> aggPreMergeCompPageSpout(Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(EXECUTOR_ID, beat.get("exec-id"));
        ret.put(HOST, beat.get(HOST));
        ret.put(PORT, beat.get(PORT));
        ret.put(ClientStatsUtil.UPTIME, beat.get(ClientStatsUtil.UPTIME));
        ret.put(NUM_EXECUTORS, 1);
        ret.put(NUM_TASKS, beat.get(NUM_TASKS));

        Map stat2win2sid2num = ClientStatsUtil.getMapByKey(beat, STATS);

        // calc sid->output-stats
        Map outputStats = new HashMap();
        Map win2sid2acked = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, ACKED), TO_STRING);
        Map win2sid2failed = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, FAILED), TO_STRING);
        Map win2sid2emitted = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EMITTED), TO_STRING);

        outputStats.put(ACKED, win2sid2acked.get(window));
        outputStats.put(FAILED, win2sid2failed.get(window));
        Map<String, Long> sid2emitted = (Map) win2sid2emitted.get(window);
        if (sid2emitted == null) {
            sid2emitted = new HashMap<>();
        }
        outputStats.put(EMITTED, filterSysStreams2Stat(sid2emitted, includeSys));

        Map win2sid2transferred = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, TRANSFERRED), TO_STRING);
        Map<String, Long> sid2transferred = (Map) win2sid2transferred.get(window);
        if (sid2transferred == null) {
            sid2transferred = new HashMap<>();
        }
        outputStats.put(TRANSFERRED, filterSysStreams2Stat(sid2transferred, includeSys));
        outputStats = swapMapOrder(outputStats);

        Map win2sid2compLat = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, COMP_LATENCIES), TO_STRING);
        Map sid2compLat = (Map) win2sid2compLat.get(window);
        Map sid2acked = (Map) win2sid2acked.get(window);
        mergeMaps(outputStats, aggSpoutStreamsLatAndCount(sid2compLat, sid2acked));
        ret.put(SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    /**
     * pre-merge component stats of specified bolt id.
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return { comp id -> comp-stats }
     */
    public static <K, V extends Number> Map<String, Object> aggPreMergeTopoPageBolt(
        Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> subRet = new HashMap<>();
        subRet.put(NUM_EXECUTORS, 1);
        subRet.put(NUM_TASKS, beat.get(NUM_TASKS));

        Map<String, Object> stat2win2sid2num = ClientStatsUtil.getMapByKey(beat, STATS);
        subRet.put(CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeyOr0(beat, ClientStatsUtil.UPTIME).intValue()));

        for (String key : new String[]{ EMITTED, TRANSFERRED, ACKED, FAILED }) {
            Map<String, Map<K, V>> stat = windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, key), TO_STRING);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            Map<K, V> winStat = stat.get(window);
            long sum = 0;
            if (winStat != null) {
                for (V v : winStat.values()) {
                    sum += v.longValue();
                }
            }
            subRet.put(key, sum);
        }

        Map<String, Map<List<String>, Double>> win2sid2execLat =
            windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EXEC_LATENCIES), TO_STRING);
        Map<String, Map<List<String>, Double>> win2sid2procLat =
            windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, PROC_LATENCIES), TO_STRING);
        Map<String, Map<List<String>, Long>> win2sid2exec =
            windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, EXECUTED), TO_STRING);
        subRet.putAll(aggBoltLatAndCount(
            win2sid2execLat.get(window), win2sid2procLat.get(window), win2sid2exec.get(window)));

        Map<String, Object> ret = new HashMap<>();
        ret.put((String) beat.get("comp-id"), subRet);
        return ret;
    }

    /**
     * pre-merge component stats of specified spout id and returns { comp id -> comp-stats }.
     */
    public static <K, V extends Number> Map<String, Object> aggPreMergeTopoPageSpout(
        Map<String, Object> m, String window, boolean includeSys) {
        Map<String, Object> subRet = new HashMap<>();
        subRet.put(NUM_EXECUTORS, 1);
        subRet.put(NUM_TASKS, m.get(NUM_TASKS));

        // no capacity for spout
        Map<String, Map<String, Map<String, V>>> stat2win2sid2num = ClientStatsUtil.getMapByKey(m, STATS);
        for (String key : new String[]{ EMITTED, TRANSFERRED, FAILED }) {
            Map<String, Map<K, V>> stat = windowSetConverter(stat2win2sid2num.get(key), TO_STRING);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            Map<K, V> winStat = stat.get(window);
            long sum = 0;
            if (winStat != null) {
                for (V v : winStat.values()) {
                    sum += v.longValue();
                }
            }
            subRet.put(key, sum);
        }

        Map<String, Map<String, Double>> win2sid2compLat =
            windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, COMP_LATENCIES), TO_STRING);
        Map<String, Map<String, Long>> win2sid2acked =
            windowSetConverter(ClientStatsUtil.getMapByKey(stat2win2sid2num, ACKED), TO_STRING);
        subRet.putAll(aggSpoutLatAndCount(win2sid2compLat.get(window), win2sid2acked.get(window)));

        Map<String, Object> ret = new HashMap<>();
        ret.put((String) m.get("comp-id"), subRet);
        return ret;
    }

    /**
     * merge accumulated bolt stats with pre-merged component stats.
     *
     * @param accBoltStats accumulated bolt stats
     * @param boltStats    pre-merged component stats
     * @return merged stats
     */
    public static Map<String, Object> mergeAggCompStatsCompPageBolt(
        Map<String, Object> accBoltStats, Map<String, Object> boltStats) {
        Map<String, Object> ret = new HashMap<>();

        Map<List<String>, Map<String, ?>> accIn = ClientStatsUtil.getMapByKey(accBoltStats, CID_SID_TO_IN_STATS);
        Map<String, Map<String, ?>> accOut = ClientStatsUtil.getMapByKey(accBoltStats, SID_TO_OUT_STATS);
        Map<List<String>, Map<String, ?>> boltIn = ClientStatsUtil.getMapByKey(boltStats, CID_SID_TO_IN_STATS);
        Map<String, Map<String, ?>> boltOut = ClientStatsUtil.getMapByKey(boltStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeyOr0(accBoltStats, NUM_EXECUTORS).intValue();
        ret.put(NUM_EXECUTORS, numExecutors + 1);
        ret.put(NUM_TASKS, sumOr0(
            getByKeyOr0(accBoltStats, NUM_TASKS), getByKeyOr0(boltStats, NUM_TASKS)));

        // (merge-with (partial merge-with sum-or-0) acc-out spout-out)
        ret.put(SID_TO_OUT_STATS, fullMergeWithSum(accOut, boltOut));
        // {component id -> metric -> value}, note that input may contain both long and double values
        ret.put(CID_SID_TO_IN_STATS, fullMergeWithSum(accIn, boltIn));

        long executed = sumStreamsLong(boltIn, EXECUTED);
        ret.put(EXECUTED, executed);

        Map<String, Object> executorStats = new HashMap<>();
        executorStats.put(EXECUTOR_ID, boltStats.get(EXECUTOR_ID));
        executorStats.put(ClientStatsUtil.UPTIME, boltStats.get(ClientStatsUtil.UPTIME));
        executorStats.put(HOST, boltStats.get(HOST));
        executorStats.put(PORT, boltStats.get(PORT));
        executorStats.put(CAPACITY, boltStats.get(CAPACITY));

        executorStats.put(EMITTED, sumStreamsLong(boltOut, EMITTED));
        executorStats.put(TRANSFERRED, sumStreamsLong(boltOut, TRANSFERRED));
        executorStats.put(ACKED, sumStreamsLong(boltIn, ACKED));
        executorStats.put(FAILED, sumStreamsLong(boltIn, FAILED));
        executorStats.put(EXECUTED, executed);

        if (executed > 0) {
            executorStats.put(EXEC_LATENCY, sumStreamsDouble(boltIn, EXEC_LAT_TOTAL) / executed);
            executorStats.put(PROC_LATENCY, sumStreamsDouble(boltIn, PROC_LAT_TOTAL) / executed);
        } else {
            executorStats.put(EXEC_LATENCY, null);
            executorStats.put(PROC_LATENCY, null);
        }
        List executorStatsList = ((List) accBoltStats.get(ClientStatsUtil.EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        ret.put(ClientStatsUtil.EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    /**
     * merge accumulated bolt stats with pre-merged component stats.
     */
    public static Map<String, Object> mergeAggCompStatsCompPageSpout(
        Map<String, Object> accSpoutStats, Map<String, Object> spoutStats) {
        Map<String, Object> ret = new HashMap<>();

        // {stream id -> metric -> value}, note that sid->out-stats may contain both long and double values
        Map<String, Map<String, ?>> accOut = ClientStatsUtil.getMapByKey(accSpoutStats, SID_TO_OUT_STATS);
        Map<String, Map<String, ?>> spoutOut = ClientStatsUtil.getMapByKey(spoutStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeyOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        ret.put(NUM_EXECUTORS, numExecutors + 1);
        ret.put(NUM_TASKS, sumOr0(
            getByKeyOr0(accSpoutStats, NUM_TASKS), getByKeyOr0(spoutStats, NUM_TASKS)));
        ret.put(SID_TO_OUT_STATS, fullMergeWithSum(accOut, spoutOut));

        Map executorStats = new HashMap();
        executorStats.put(EXECUTOR_ID, spoutStats.get(EXECUTOR_ID));
        executorStats.put(ClientStatsUtil.UPTIME, spoutStats.get(ClientStatsUtil.UPTIME));
        executorStats.put(HOST, spoutStats.get(HOST));
        executorStats.put(PORT, spoutStats.get(PORT));

        executorStats.put(EMITTED, sumStreamsLong(spoutOut, EMITTED));
        executorStats.put(TRANSFERRED, sumStreamsLong(spoutOut, TRANSFERRED));
        executorStats.put(FAILED, sumStreamsLong(spoutOut, FAILED));
        long acked = sumStreamsLong(spoutOut, ACKED);
        executorStats.put(ACKED, acked);
        if (acked > 0) {
            executorStats.put(COMP_LATENCY, sumStreamsDouble(spoutOut, COMP_LAT_TOTAL) / acked);
        } else {
            executorStats.put(COMP_LATENCY, null);
        }
        List executorStatsList = ((List) accSpoutStats.get(ClientStatsUtil.EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        ret.put(ClientStatsUtil.EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    /**
     * merge accumulated bolt stats with new bolt stats.
     *
     * @param accBoltStats accumulated bolt stats
     * @param boltStats    new input bolt stats
     * @return merged bolt stats
     */
    public static Map<String, Object> mergeAggCompStatsTopoPageBolt(Map<String, Object> accBoltStats,
                                                                    Map<String, Object> boltStats) {
        Map<String, Object> ret = new HashMap<>();

        Integer numExecutors = getByKeyOr0(accBoltStats, NUM_EXECUTORS).intValue();
        ret.put(NUM_EXECUTORS, numExecutors + 1);
        ret.put(NUM_TASKS, sumOr0(getByKeyOr0(accBoltStats, NUM_TASKS), getByKeyOr0(boltStats, NUM_TASKS)));
        ret.put(EMITTED, sumOr0(getByKeyOr0(accBoltStats, EMITTED), getByKeyOr0(boltStats, EMITTED)));
        ret.put(TRANSFERRED, sumOr0(getByKeyOr0(accBoltStats, TRANSFERRED), getByKeyOr0(boltStats, TRANSFERRED)));
        ret.put(EXEC_LAT_TOTAL, sumOr0(getByKeyOr0(accBoltStats, EXEC_LAT_TOTAL), getByKeyOr0(boltStats, EXEC_LAT_TOTAL)));
        ret.put(PROC_LAT_TOTAL, sumOr0(getByKeyOr0(accBoltStats, PROC_LAT_TOTAL), getByKeyOr0(boltStats, PROC_LAT_TOTAL)));
        ret.put(EXECUTED, sumOr0(getByKeyOr0(accBoltStats, EXECUTED), getByKeyOr0(boltStats, EXECUTED)));
        ret.put(ACKED, sumOr0(getByKeyOr0(accBoltStats, ACKED), getByKeyOr0(boltStats, ACKED)));
        ret.put(FAILED, sumOr0(getByKeyOr0(accBoltStats, FAILED), getByKeyOr0(boltStats, FAILED)));
        ret.put(CAPACITY, maxOr0(getByKeyOr0(accBoltStats, CAPACITY), getByKeyOr0(boltStats, CAPACITY)));

        return ret;
    }

    /**
     * merge accumulated bolt stats with new bolt stats.
     */
    public static Map<String, Object> mergeAggCompStatsTopoPageSpout(Map<String, Object> accSpoutStats,
                                                                     Map<String, Object> spoutStats) {
        Map<String, Object> ret = new HashMap<>();

        Integer numExecutors = getByKeyOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        ret.put(NUM_EXECUTORS, numExecutors + 1);
        ret.put(NUM_TASKS, sumOr0(getByKeyOr0(accSpoutStats, NUM_TASKS), getByKeyOr0(spoutStats, NUM_TASKS)));
        ret.put(EMITTED, sumOr0(getByKeyOr0(accSpoutStats, EMITTED), getByKeyOr0(spoutStats, EMITTED)));
        ret.put(TRANSFERRED, sumOr0(getByKeyOr0(accSpoutStats, TRANSFERRED), getByKeyOr0(spoutStats, TRANSFERRED)));
        ret.put(COMP_LAT_TOTAL, sumOr0(getByKeyOr0(accSpoutStats, COMP_LAT_TOTAL), getByKeyOr0(spoutStats, COMP_LAT_TOTAL)));
        ret.put(ACKED, sumOr0(getByKeyOr0(accSpoutStats, ACKED), getByKeyOr0(spoutStats, ACKED)));
        ret.put(FAILED, sumOr0(getByKeyOr0(accSpoutStats, FAILED), getByKeyOr0(spoutStats, FAILED)));

        return ret;
    }

    /**
     * A helper function that does the common work to aggregate stats of one executor with the given map for the topology page.
     */
    public static Map<String, Object> aggTopoExecStats(
        String window, boolean includeSys, Map<String, Object> accStats, Map<String, Object> beat, String compType) {
        boolean isSpout = compType.equals(ClientStatsUtil.SPOUT);
        // component id -> stats
        Map<String, Object> cid2stats;
        if (isSpout) {
            cid2stats = aggPreMergeTopoPageSpout(beat, window, includeSys);
        } else {
            cid2stats = aggPreMergeTopoPageBolt(beat, window, includeSys);
        }

        Map stats = ClientStatsUtil.getMapByKey(beat, STATS);
        Map w2compLatWgtAvg;
        Map w2acked;
        Map compLatStats = ClientStatsUtil.getMapByKey(stats, COMP_LATENCIES);
        if (isSpout) { // agg spout stats
            Map mm = new HashMap();

            Map acked = ClientStatsUtil.getMapByKey(stats, ACKED);
            for (Object win : acked.keySet()) {
                mm.put(win, aggSpoutLatAndCount((Map) compLatStats.get(win), (Map) acked.get(win)));
            }
            mm = swapMapOrder(mm);
            w2compLatWgtAvg = ClientStatsUtil.getMapByKey(mm, COMP_LAT_TOTAL);
            w2acked = ClientStatsUtil.getMapByKey(mm, ACKED);
        } else {
            w2compLatWgtAvg = null;
            w2acked = aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, ACKED));
        }

        Map<String, Object> ret = new HashMap<>();
        Set workerSet = (Set) accStats.get(WORKERS_SET);
        workerSet.add(Lists.newArrayList(beat.get(HOST), beat.get(PORT)));
        ret.put(WORKERS_SET, workerSet);

        Map bolt2stats = ClientStatsUtil.getMapByKey(accStats, BOLT_TO_STATS);
        ret.put(BOLT_TO_STATS, bolt2stats);

        Map spout2stats = ClientStatsUtil.getMapByKey(accStats, SPOUT_TO_STATS);
        ret.put(SPOUT_TO_STATS, spout2stats);

        Map win2emitted = ClientStatsUtil.getMapByKey(accStats, WIN_TO_EMITTED);
        ret.put(WIN_TO_EMITTED, mergeWithSumLong(win2emitted, aggregateCountStreams(
            filterSysStreams(ClientStatsUtil.getMapByKey(stats, EMITTED), includeSys))));

        Map win2transferred = ClientStatsUtil.getMapByKey(accStats, WIN_TO_TRANSFERRED);
        ret.put(WIN_TO_TRANSFERRED, mergeWithSumLong(win2transferred, aggregateCountStreams(
            filterSysStreams(ClientStatsUtil.getMapByKey(stats, TRANSFERRED), includeSys))));

        Map win2compLatWgtAvg = ClientStatsUtil.getMapByKey(accStats, WIN_TO_COMP_LAT_WGT_AVG);
        ret.put(WIN_TO_COMP_LAT_WGT_AVG, mergeWithSumDouble(win2compLatWgtAvg, w2compLatWgtAvg));

        Map win2acked = ClientStatsUtil.getMapByKey(accStats, WIN_TO_ACKED);
        Object v1 = isSpout ? mergeWithSumLong(win2acked, w2acked) : win2acked;
        ret.put(WIN_TO_ACKED, v1);

        Map win2failed = ClientStatsUtil.getMapByKey(accStats, WIN_TO_FAILED);
        Object v = isSpout
            ? mergeWithSumLong(aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, FAILED)), win2failed) : win2failed;

        ret.put(WIN_TO_FAILED, v);
        ret.put(TYPE, stats.get(TYPE));

        // (merge-with merge-agg-comp-stats-topo-page-bolt/spout (acc-stats comp-key) cid->statk->num)
        // (acc-stats comp-key) ==> bolt2stats/spout2stats
        if (isSpout) {
            for (String spout : cid2stats.keySet()) {
                spout2stats.put(spout, mergeAggCompStatsTopoPageSpout((Map) spout2stats.get(spout), (Map) cid2stats.get(spout)));
            }
        } else {
            for (String bolt : cid2stats.keySet()) {
                bolt2stats.put(bolt, mergeAggCompStatsTopoPageBolt((Map) bolt2stats.get(bolt), (Map) cid2stats.get(bolt)));
            }
        }

        return ret;
    }

    /**
     * aggregate topo executors stats.
     *
     * @param topologyId     topology id
     * @param exec2nodePort  executor -> host+port
     * @param task2component task -> component
     * @param beats          executor[start, end] -> executor heartbeat
     * @param topology       storm topology
     * @param window         the window to be aggregated
     * @param includeSys     whether to include system streams
     * @param clusterState   cluster state
     * @return TopologyPageInfo thrift structure
     */
    public static TopologyPageInfo aggTopoExecsStats(
        String topologyId, Map exec2nodePort, Map task2component, Map<List<Integer>, Map<String, Object>> beats,
        StormTopology topology, String window, boolean includeSys, IStormClusterState clusterState) {
        List<Map<String, Object>> beatList = extractDataFromHb(exec2nodePort, task2component, beats, includeSys, topology);
        Map<String, Object> topoStats = aggregateTopoStats(window, includeSys, beatList);
        return postAggregateTopoStats(task2component, exec2nodePort, topoStats, topologyId, clusterState);
    }

    private static Map<String, Object> aggregateTopoStats(String win, boolean includeSys, List<Map<String, Object>> heartbeats) {
        Map<String, Object> initVal = new HashMap<>();
        initVal.put(WORKERS_SET, new HashSet());
        initVal.put(BOLT_TO_STATS, new HashMap());
        initVal.put(SPOUT_TO_STATS, new HashMap());
        initVal.put(WIN_TO_EMITTED, new HashMap());
        initVal.put(WIN_TO_TRANSFERRED, new HashMap());
        initVal.put(WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        initVal.put(WIN_TO_ACKED, new HashMap());
        initVal.put(WIN_TO_FAILED, new HashMap());

        for (Map<String, Object> heartbeat : heartbeats) {
            String compType = (String) heartbeat.get(TYPE);
            initVal = aggTopoExecStats(win, includeSys, initVal, heartbeat, compType);
        }

        return initVal;
    }

    private static TopologyPageInfo postAggregateTopoStats(Map task2comp, Map exec2nodePort, Map<String, Object> accData,
                                                          String topologyId, IStormClusterState clusterState) {
        TopologyPageInfo ret = new TopologyPageInfo(topologyId);

        ret.set_num_tasks(task2comp.size());
        ret.set_num_workers(((Set) accData.get(WORKERS_SET)).size());
        ret.set_num_executors(exec2nodePort != null ? exec2nodePort.size() : 0);

        Map bolt2stats = ClientStatsUtil.getMapByKey(accData, BOLT_TO_STATS);
        Map<String, ComponentAggregateStats> aggBolt2stats = new HashMap<>();
        for (Object o : bolt2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            Map m = (Map) e.getValue();
            long executed = getByKeyOr0(m, EXECUTED).longValue();
            if (executed > 0) {
                double execLatencyTotal = getByKeyOr0(m, EXEC_LAT_TOTAL).doubleValue();
                m.put(EXEC_LATENCY, execLatencyTotal / executed);

                double procLatencyTotal = getByKeyOr0(m, PROC_LAT_TOTAL).doubleValue();
                m.put(PROC_LATENCY, procLatencyTotal / executed);
            }
            m.remove(EXEC_LAT_TOTAL);
            m.remove(PROC_LAT_TOTAL);
            String id = (String) e.getKey();
            m.put(LAST_ERROR, getLastError(clusterState, topologyId, id));

            aggBolt2stats.put(id, thriftifyBoltAggStats(m));
        }

        Map spout2stats = ClientStatsUtil.getMapByKey(accData, SPOUT_TO_STATS);
        Map<String, ComponentAggregateStats> aggSpout2stats = new HashMap<>();
        for (Object o : spout2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map m = (Map) e.getValue();
            long acked = getByKeyOr0(m, ACKED).longValue();
            if (acked > 0) {
                double compLatencyTotal = getByKeyOr0(m, COMP_LAT_TOTAL).doubleValue();
                m.put(COMP_LATENCY, compLatencyTotal / acked);
            }
            m.remove(COMP_LAT_TOTAL);
            m.put(LAST_ERROR, getLastError(clusterState, topologyId, id));

            aggSpout2stats.put(id, thriftifySpoutAggStats(m));
        }

        TopologyStats topologyStats = new TopologyStats();
        topologyStats.set_window_to_acked(mapKeyStr(ClientStatsUtil.getMapByKey(accData, WIN_TO_ACKED)));
        topologyStats.set_window_to_emitted(mapKeyStr(ClientStatsUtil.getMapByKey(accData, WIN_TO_EMITTED)));
        topologyStats.set_window_to_failed(mapKeyStr(ClientStatsUtil.getMapByKey(accData, WIN_TO_FAILED)));
        topologyStats.set_window_to_transferred(mapKeyStr(ClientStatsUtil.getMapByKey(accData, WIN_TO_TRANSFERRED)));
        topologyStats.set_window_to_complete_latencies_ms(computeWeightedAveragesPerWindow(
            accData, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));

        ret.set_topology_stats(topologyStats);
        ret.set_id_to_spout_agg_stats(aggSpout2stats);
        ret.set_id_to_bolt_agg_stats(aggBolt2stats);

        return ret;
    }

    /**
     * aggregate bolt stats.
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated bolt stats: {metric -> win -> global stream id -> value}
     */
    public static <T> Map<String, Map> aggregateBoltStats(List<ExecutorSummary> statsSeq, boolean includeSys) {
        Map<String, Map> ret = new HashMap<>();

        Map<String, Map<String, Map<T, Long>>> commonStats = aggregateCommonStats(statsSeq);
        // filter sys streams if necessary
        commonStats = preProcessStreamSummary(commonStats, includeSys);

        List<Map<String, Map<GlobalStreamId, Long>>> acked = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Long>>> failed = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Long>>> executed = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Double>>> processLatencies = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Double>>> executeLatencies = new ArrayList<>();
        for (ExecutorSummary summary : statsSeq) {
            ExecutorStats stat = summary.get_stats();
            acked.add(stat.get_specific().get_bolt().get_acked());
            failed.add(stat.get_specific().get_bolt().get_failed());
            executed.add(stat.get_specific().get_bolt().get_executed());
            processLatencies.add(stat.get_specific().get_bolt().get_process_ms_avg());
            executeLatencies.add(stat.get_specific().get_bolt().get_execute_ms_avg());
        }
        mergeMaps(ret, commonStats);
        ((Map) ret).put(ACKED, aggregateCounts(acked));
        ((Map) ret).put(FAILED, aggregateCounts(failed));
        ((Map) ret).put(EXECUTED, aggregateCounts(executed));
        ((Map) ret).put(PROC_LATENCIES, aggregateAverages(processLatencies, acked));
        ((Map) ret).put(EXEC_LATENCIES, aggregateAverages(executeLatencies, executed));

        return ret;
    }

    /**
     * aggregate spout stats.
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated spout stats: {metric -> win -> global stream id -> value}
     */
    public static Map<String, Map> aggregateSpoutStats(List<ExecutorSummary> statsSeq, boolean includeSys) {
        // actually Map<String, Map<String, Map<String, Long/Double>>>
        Map<String, Map> ret = new HashMap<>();

        Map<String, Map<String, Map<String, Long>>> commonStats = aggregateCommonStats(statsSeq);
        // filter sys streams if necessary
        commonStats = preProcessStreamSummary(commonStats, includeSys);

        List<Map<String, Map<String, Long>>> acked = new ArrayList<>();
        List<Map<String, Map<String, Long>>> failed = new ArrayList<>();
        List<Map<String, Map<String, Double>>> completeLatencies = new ArrayList<>();
        for (ExecutorSummary summary : statsSeq) {
            ExecutorStats stats = summary.get_stats();
            acked.add(stats.get_specific().get_spout().get_acked());
            failed.add(stats.get_specific().get_spout().get_failed());
            completeLatencies.add(stats.get_specific().get_spout().get_complete_ms_avg());
        }
        ret.putAll(commonStats);
        ((Map) ret).put(ACKED, aggregateCounts(acked));
        ((Map) ret).put(FAILED, aggregateCounts(failed));
        ((Map) ret).put(COMP_LATENCIES, aggregateAverages(completeLatencies, acked));

        return ret;
    }

    /**
     * aggregate common stats from a spout/bolt, called in aggregateSpoutStats/aggregateBoltStats.
     */
    public static <T> Map<String, Map<String, Map<T, Long>>> aggregateCommonStats(List<ExecutorSummary> statsSeq) {
        Map<String, Map<String, Map<T, Long>>> ret = new HashMap<>();

        List<Map<String, Map<String, Long>>> emitted = new ArrayList<>();
        List<Map<String, Map<String, Long>>> transferred = new ArrayList<>();
        for (ExecutorSummary summ : statsSeq) {
            emitted.add(summ.get_stats().get_emitted());
            transferred.add(summ.get_stats().get_transferred());
        }
        ((Map) ret).put(EMITTED, aggregateCounts(emitted));
        ((Map) ret).put(TRANSFERRED, aggregateCounts(transferred));

        return ret;
    }

    /**
     * filter system streams of aggregated spout/bolt stats if necessary.
     */
    public static <T> Map<String, Map<String, Map<T, Long>>> preProcessStreamSummary(
        Map<String, Map<String, Map<T, Long>>> streamSummary, boolean includeSys) {
        Map<String, Map<T, Long>> emitted = ClientStatsUtil.getMapByKey(streamSummary, EMITTED);
        Map<String, Map<T, Long>> transferred = ClientStatsUtil.getMapByKey(streamSummary, TRANSFERRED);

        ((Map) streamSummary).put(EMITTED, filterSysStreams(emitted, includeSys));
        ((Map) streamSummary).put(TRANSFERRED, filterSysStreams(transferred, includeSys));

        return streamSummary;
    }

    /**
     * aggregate count streams by window.
     *
     * @param stats a Map of value: {win -> stream -> value}
     * @return a Map of value: {win -> value}
     */
    public static <K, V extends Number> Map<String, Long> aggregateCountStreams(
        Map<String, Map<K, V>> stats) {
        Map<String, Long> ret = new HashMap<>();
        for (Map.Entry<String, Map<K, V>> entry : stats.entrySet()) {
            Map<K, V> value = entry.getValue();
            long sum = 0L;
            for (V num : value.values()) {
                sum += num.longValue();
            }
            ret.put(entry.getKey(), sum);
        }
        return ret;
    }

    /**
     * compute an weighted average from a list of average maps and a corresponding count maps extracted from a list of ExecutorSummary.
     *
     * @param avgSeq   a list of {win -> global stream id -> avg value}
     * @param countSeq a list of {win -> global stream id -> count value}
     * @return a Map of {win -> global stream id -> weighted avg value}
     */
    public static <K> Map<String, Map<K, Double>> aggregateAverages(List<Map<String, Map<K, Double>>> avgSeq,
                                                                    List<Map<String, Map<K, Long>>> countSeq) {
        Map<String, Map<K, Double>> ret = new HashMap<>();

        Map<String, Map<K, List>> expands = expandAveragesSeq(avgSeq, countSeq);
        if (expands == null) {
            return ret;
        }
        for (Map.Entry<String, Map<K, List>> entry : expands.entrySet()) {
            String k = entry.getKey();

            Map<K, Double> tmp = new HashMap<>();
            Map<K, List> inner = entry.getValue();
            for (K kk : inner.keySet()) {
                List vv = inner.get(kk);
                tmp.put(kk, valAvg(((Number) vv.get(0)).doubleValue(), ((Number) vv.get(1)).longValue()));
            }
            ret.put(k, tmp);
        }

        return ret;
    }

    /**
     * aggregate weighted average of all streams.
     *
     * @param avgs   a Map of {win -> stream -> average value}
     * @param counts a Map of {win -> stream -> count value}
     * @return a Map of {win -> aggregated value}
     */
    public static <K> Map<String, Double> aggregateAvgStreams(Map<String, Map<K, Double>> avgs,
                                                              Map<String, Map<K, Long>> counts) {
        Map<String, Double> ret = new HashMap<>();

        Map<String, Map<K, List>> expands = expandAverages(avgs, counts);
        for (Map.Entry<String, Map<K, List>> entry : expands.entrySet()) {
            String win = entry.getKey();

            double avgTotal = 0.0;
            long cntTotal = 0L;
            Map<K, List> inner = entry.getValue();
            for (K kk : inner.keySet()) {
                List vv = inner.get(kk);
                avgTotal += ((Number) vv.get(0)).doubleValue();
                cntTotal += ((Number) vv.get(1)).longValue();
            }
            ret.put(win, valAvg(avgTotal, cntTotal));
        }

        return ret;
    }

    /**
     * aggregates spout stream stats, returns a Map of {metric -> win -> aggregated value}.
     */
    public static Map<String, Map> spoutStreamsStats(List<ExecutorSummary> summs, boolean includeSys) {
        if (summs == null) {
            return new HashMap<>();
        }
        // filter ExecutorSummary's with empty stats
        List<ExecutorSummary> statsSeq = getFilledStats(summs);
        return aggregateSpoutStreams(aggregateSpoutStats(statsSeq, includeSys));
    }

    /**
     * aggregates bolt stream stats, returns a Map of {metric -> win -> aggregated value}.
     */
    public static Map<String, Map> boltStreamsStats(List<ExecutorSummary> summs, boolean includeSys) {
        if (summs == null) {
            return new HashMap<>();
        }
        List<ExecutorSummary> statsSeq = getFilledStats(summs);
        return aggregateBoltStreams(aggregateBoltStats(statsSeq, includeSys));
    }

    /**
     * aggregate all spout streams.
     *
     * @param stats a Map of {metric -> win -> stream id -> value}
     * @return a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> aggregateSpoutStreams(Map<String, Map> stats) {
        // actual ret is Map<String, Map<String, Long/Double>>
        Map<String, Map> ret = new HashMap<>();
        ((Map) ret).put(ACKED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, ACKED)));
        ((Map) ret).put(FAILED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, FAILED)));
        ((Map) ret).put(EMITTED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, EMITTED)));
        ((Map) ret).put(TRANSFERRED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, TRANSFERRED)));
        ((Map) ret).put(COMP_LATENCIES, aggregateAvgStreams(
            ClientStatsUtil.getMapByKey(stats, COMP_LATENCIES), ClientStatsUtil.getMapByKey(stats, ACKED)));
        return ret;
    }

    /**
     * aggregate all bolt streams.
     *
     * @param stats a Map of {metric -> win -> stream id -> value}
     * @return a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> aggregateBoltStreams(Map<String, Map> stats) {
        Map<String, Map> ret = new HashMap<>();
        ((Map) ret).put(ACKED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, ACKED)));
        ((Map) ret).put(FAILED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, FAILED)));
        ((Map) ret).put(EMITTED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, EMITTED)));
        ((Map) ret).put(TRANSFERRED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, TRANSFERRED)));
        ((Map) ret).put(EXECUTED, aggregateCountStreams(ClientStatsUtil.getMapByKey(stats, EXECUTED)));
        ((Map) ret).put(PROC_LATENCIES, aggregateAvgStreams(
            ClientStatsUtil.getMapByKey(stats, PROC_LATENCIES), ClientStatsUtil.getMapByKey(stats, ACKED)));
        ((Map) ret).put(EXEC_LATENCIES, aggregateAvgStreams(
            ClientStatsUtil.getMapByKey(stats, EXEC_LATENCIES), ClientStatsUtil.getMapByKey(stats, EXECUTED)));
        return ret;
    }

    /**
     * aggregate windowed stats from a bolt executor stats with a Map of accumulated stats.
     */
    public static Map<String, Object> aggBoltExecWinStats(
        Map<String, Object> accStats, Map<String, Object> newStats, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Map<String, Number>> m = new HashMap<>();
        for (Object win : ClientStatsUtil.getMapByKey(newStats, EXECUTED).keySet()) {
            m.put((String) win, aggBoltLatAndCount(
                (Map) (ClientStatsUtil.getMapByKey(newStats, EXEC_LATENCIES)).get(win),
                (Map) (ClientStatsUtil.getMapByKey(newStats, PROC_LATENCIES)).get(win),
                (Map) (ClientStatsUtil.getMapByKey(newStats, EXECUTED)).get(win)));
        }
        m = swapMapOrder(m);

        Map<String, Double> win2execLatWgtAvg = ClientStatsUtil.getMapByKey(m, EXEC_LAT_TOTAL);
        Map<String, Double> win2procLatWgtAvg = ClientStatsUtil.getMapByKey(m, PROC_LAT_TOTAL);
        Map<String, Long> win2executed = ClientStatsUtil.getMapByKey(m, EXECUTED);

        Map<String, Map<String, Long>> emitted = ClientStatsUtil.getMapByKey(newStats, EMITTED);
        Map<String, Long> win2emitted = mergeWithSumLong(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                                                         ClientStatsUtil.getMapByKey(accStats, WIN_TO_EMITTED));
        ret.put(WIN_TO_EMITTED, win2emitted);

        Map<String, Map<String, Long>> transferred = ClientStatsUtil.getMapByKey(newStats, TRANSFERRED);
        Map<String, Long> win2transferred = mergeWithSumLong(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                                                             ClientStatsUtil.getMapByKey(accStats, WIN_TO_TRANSFERRED));
        ret.put(WIN_TO_TRANSFERRED, win2transferred);

        ret.put(WIN_TO_EXEC_LAT_WGT_AVG, mergeWithSumDouble(
            ClientStatsUtil.getMapByKey(accStats, WIN_TO_EXEC_LAT_WGT_AVG), win2execLatWgtAvg));
        ret.put(WIN_TO_PROC_LAT_WGT_AVG, mergeWithSumDouble(
            ClientStatsUtil.getMapByKey(accStats, WIN_TO_PROC_LAT_WGT_AVG), win2procLatWgtAvg));
        ret.put(WIN_TO_EXECUTED, mergeWithSumLong(
            ClientStatsUtil.getMapByKey(accStats, WIN_TO_EXECUTED), win2executed));
        ret.put(WIN_TO_ACKED, mergeWithSumLong(
            aggregateCountStreams(ClientStatsUtil.getMapByKey(newStats, ACKED)), ClientStatsUtil.getMapByKey(accStats, WIN_TO_ACKED)));
        ret.put(WIN_TO_FAILED, mergeWithSumLong(
            aggregateCountStreams(ClientStatsUtil.getMapByKey(newStats, FAILED)), ClientStatsUtil.getMapByKey(accStats, WIN_TO_FAILED)));

        return ret;
    }

    /**
     * aggregate windowed stats from a spout executor stats with a Map of accumulated stats.
     */
    public static Map<String, Object> aggSpoutExecWinStats(
        Map<String, Object> accStats, Map<String, Object> beat, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Map<String, Number>> m = new HashMap<>();
        for (Object win : ClientStatsUtil.getMapByKey(beat, ACKED).keySet()) {
            m.put((String) win, aggSpoutLatAndCount(
                (Map<String, Double>) (ClientStatsUtil.getMapByKey(beat, COMP_LATENCIES)).get(win),
                (Map<String, Long>) (ClientStatsUtil.getMapByKey(beat, ACKED)).get(win)));
        }
        m = swapMapOrder(m);

        Map<String, Double> win2compLatWgtAvg = ClientStatsUtil.getMapByKey(m, COMP_LAT_TOTAL);
        Map<String, Long> win2acked = ClientStatsUtil.getMapByKey(m, ACKED);

        Map<String, Map<String, Long>> emitted = ClientStatsUtil.getMapByKey(beat, EMITTED);
        Map<String, Long> win2emitted = mergeWithSumLong(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                                                         ClientStatsUtil.getMapByKey(accStats, WIN_TO_EMITTED));
        ret.put(WIN_TO_EMITTED, win2emitted);

        Map<String, Map<String, Long>> transferred = ClientStatsUtil.getMapByKey(beat, TRANSFERRED);
        Map<String, Long> win2transferred = mergeWithSumLong(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                                                             ClientStatsUtil.getMapByKey(accStats, WIN_TO_TRANSFERRED));
        ret.put(WIN_TO_TRANSFERRED, win2transferred);

        ret.put(WIN_TO_COMP_LAT_WGT_AVG, mergeWithSumDouble(
            ClientStatsUtil.getMapByKey(accStats, WIN_TO_COMP_LAT_WGT_AVG), win2compLatWgtAvg));
        ret.put(WIN_TO_ACKED, mergeWithSumLong(
            ClientStatsUtil.getMapByKey(accStats, WIN_TO_ACKED), win2acked));
        ret.put(WIN_TO_FAILED, mergeWithSumLong(
            aggregateCountStreams(ClientStatsUtil.getMapByKey(beat, FAILED)), ClientStatsUtil.getMapByKey(accStats, WIN_TO_FAILED)));

        return ret;
    }


    /**
     * aggregate a list of count maps into one map.
     *
     * @param countsSeq a seq of {win -> GlobalStreamId -> value}
     */
    public static <T> Map<String, Map<T, Long>> aggregateCounts(List<Map<String, Map<T, Long>>> countsSeq) {
        Map<String, Map<T, Long>> ret = new HashMap<>();
        for (Map<String, Map<T, Long>> counts : countsSeq) {
            for (Map.Entry<String, Map<T, Long>> entry : counts.entrySet()) {
                String win = entry.getKey();
                Map<T, Long> stream2count = entry.getValue();

                if (!ret.containsKey(win)) {
                    ret.put(win, stream2count);
                } else {
                    Map<T, Long> existing = ret.get(win);
                    for (Map.Entry<T, Long> subEntry : stream2count.entrySet()) {
                        T stream = subEntry.getKey();
                        if (!existing.containsKey(stream)) {
                            existing.put(stream, subEntry.getValue());
                        } else {
                            existing.put(stream, subEntry.getValue() + existing.get(stream));
                        }
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Aggregate the stats for a component over a given window of time.
     */
    public static Map<String, Object> aggregateCompStats(
        String window, boolean includeSys, List<Map<String, Object>> beats, String compType) {

        Map<String, Object> initVal = new HashMap<>();
        initVal.put(WIN_TO_ACKED, new HashMap());
        initVal.put(WIN_TO_FAILED, new HashMap());
        initVal.put(WIN_TO_EMITTED, new HashMap());
        initVal.put(WIN_TO_TRANSFERRED, new HashMap());

        Map<String, Object> stats = new HashMap();
        stats.put(ClientStatsUtil.EXECUTOR_STATS, new ArrayList());
        stats.put(SID_TO_OUT_STATS, new HashMap());
        boolean isSpout = ClientStatsUtil.SPOUT.equals(compType);
        if (isSpout) {
            initVal.put(TYPE, ClientStatsUtil.SPOUT);
            initVal.put(WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        } else {
            initVal.put(TYPE, ClientStatsUtil.BOLT);
            initVal.put(WIN_TO_EXECUTED, new HashMap());
            stats.put(CID_SID_TO_IN_STATS, new HashMap());
            initVal.put(WIN_TO_EXEC_LAT_WGT_AVG, new HashMap());
            initVal.put(WIN_TO_PROC_LAT_WGT_AVG, new HashMap());
        }
        initVal.put(STATS, stats);

        // iterate through all executor heartbeats
        for (Map<String, Object> beat : beats) {
            initVal = aggCompExecStats(window, includeSys, initVal, beat, compType);
        }

        return initVal;
    }

    /**
     * Combines the aggregate stats of one executor with the given map, selecting the appropriate window and including system components as
     * specified.
     */
    public static Map<String, Object> aggCompExecStats(String window, boolean includeSys, Map<String, Object> accStats,
                                                       Map<String, Object> beat, String compType) {
        Map<String, Object> ret = new HashMap<>();
        if (ClientStatsUtil.SPOUT.equals(compType)) {
            ret.putAll(aggSpoutExecWinStats(accStats, ClientStatsUtil.getMapByKey(beat, STATS), includeSys));
            ret.put(STATS, mergeAggCompStatsCompPageSpout(
                    ClientStatsUtil.getMapByKey(accStats, STATS),
                    aggPreMergeCompPageSpout(beat, window, includeSys)));
        } else {
            ret.putAll(aggBoltExecWinStats(accStats, ClientStatsUtil.getMapByKey(beat, STATS), includeSys));
            ret.put(STATS, mergeAggCompStatsCompPageBolt(
                    ClientStatsUtil.getMapByKey(accStats, STATS),
                    aggPreMergeCompPageBolt(beat, window, includeSys)));
        }
        ret.put(TYPE, compType);

        return ret;
    }

    /**
     * post aggregate component stats: 1. computes execute-latency/process-latency from execute/process latency total 2. computes windowed
     * weight avgs 3. transform Map keys
     *
     * @param compStats accumulated comp stats
     */
    public static Map<String, Object> postAggregateCompStats(Map<String, Object> compStats) {
        Map<String, Object> ret = new HashMap<>();

        String compType = (String) compStats.get(TYPE);
        Map stats = ClientStatsUtil.getMapByKey(compStats, STATS);
        Integer numTasks = getByKeyOr0(stats, NUM_TASKS).intValue();
        Integer numExecutors = getByKeyOr0(stats, NUM_EXECUTORS).intValue();
        Map outStats = ClientStatsUtil.getMapByKey(stats, SID_TO_OUT_STATS);

        ret.put(TYPE, compType);
        ret.put(NUM_TASKS, numTasks);
        ret.put(NUM_EXECUTORS, numExecutors);
        ret.put(ClientStatsUtil.EXECUTOR_STATS, stats.get(ClientStatsUtil.EXECUTOR_STATS));
        ret.put(WIN_TO_EMITTED, mapKeyStr(ClientStatsUtil.getMapByKey(compStats, WIN_TO_EMITTED)));
        ret.put(WIN_TO_TRANSFERRED, mapKeyStr(ClientStatsUtil.getMapByKey(compStats, WIN_TO_TRANSFERRED)));
        ret.put(WIN_TO_ACKED, mapKeyStr(ClientStatsUtil.getMapByKey(compStats, WIN_TO_ACKED)));
        ret.put(WIN_TO_FAILED, mapKeyStr(ClientStatsUtil.getMapByKey(compStats, WIN_TO_FAILED)));

        if (ClientStatsUtil.BOLT.equals(compType)) {
            Map inStats = ClientStatsUtil.getMapByKey(stats, CID_SID_TO_IN_STATS);

            Map inStats2 = new HashMap();
            for (Object o : inStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Map v = (Map) e.getValue();
                long executed = getByKeyOr0(v, EXECUTED).longValue();
                if (executed > 0) {
                    double executeLatencyTotal = getByKeyOr0(v, EXEC_LAT_TOTAL).doubleValue();
                    double processLatencyTotal = getByKeyOr0(v, PROC_LAT_TOTAL).doubleValue();
                    v.put(EXEC_LATENCY, executeLatencyTotal / executed);
                    v.put(PROC_LATENCY, processLatencyTotal / executed);
                } else {
                    v.put(EXEC_LATENCY, 0.0);
                    v.put(PROC_LATENCY, 0.0);
                }
                v.remove(EXEC_LAT_TOTAL);
                v.remove(PROC_LAT_TOTAL);
                inStats2.put(e.getKey(), v);
            }
            ret.put(CID_SID_TO_IN_STATS, inStats2);

            ret.put(SID_TO_OUT_STATS, outStats);
            ret.put(WIN_TO_EXECUTED, mapKeyStr(ClientStatsUtil.getMapByKey(compStats, WIN_TO_EXECUTED)));
            ret.put(WIN_TO_EXEC_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_EXEC_LAT_WGT_AVG, WIN_TO_EXECUTED));
            ret.put(WIN_TO_PROC_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_PROC_LAT_WGT_AVG, WIN_TO_EXECUTED));
        } else {
            Map outStats2 = new HashMap();
            for (Object o : outStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object k = e.getKey();
                Map v = (Map) e.getValue();
                long acked = getByKeyOr0(v, ACKED).longValue();
                if (acked > 0) {
                    double compLatencyTotal = getByKeyOr0(v, COMP_LAT_TOTAL).doubleValue();
                    v.put(COMP_LATENCY, compLatencyTotal / acked);
                } else {
                    v.put(COMP_LATENCY, 0.0);
                }
                v.remove(COMP_LAT_TOTAL);
                outStats2.put(k, v);
            }
            ret.put(SID_TO_OUT_STATS, outStats2);
            ret.put(WIN_TO_COMP_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));
        }

        return ret;
    }

    /**
     * aggregate component executor stats.
     *
     * @param exec2hostPort  a Map of {executor -> host+port}
     * @param task2component a Map of {task id -> component}
     * @param beats          a converted HashMap of executor heartbeats, {executor -> heartbeat}
     * @param window         specified window
     * @param includeSys     whether to include system streams
     * @param topologyId     topology id
     * @param topology       storm topology
     * @param componentId    component id
     * @return ComponentPageInfo thrift structure
     */
    public static ComponentPageInfo aggCompExecsStats(
        Map exec2hostPort, Map task2component, Map<List<Integer>, Map<String, Object>> beats,
        String window, boolean includeSys, String topologyId, StormTopology topology, String componentId) {

        List<Map<String, Object>> beatList =
            extractDataFromHb(exec2hostPort, task2component, beats, includeSys, topology, componentId);
        Map<String, Object> compStats = aggregateCompStats(window, includeSys, beatList, componentType(topology, componentId));
        compStats = postAggregateCompStats(compStats);
        return thriftifyCompPageData(topologyId, topology, componentId, compStats);
    }

    /**
     * aggregate statistics per worker for a topology. Optionally filtering on specific supervisors
     *
     * @param stormId          topology id
     * @param stormName        storm topology
     * @param task2Component   a Map of {task id -> component}
     * @param beats            a converted HashMap of executor heartbeats, {executor -> heartbeat}
     * @param exec2NodePort    a Map of {executor -> host+port}
     * @param includeSys       whether to include system streams
     * @param userAuthorized   whether the user is authorized to view topology info
     * @param filterSupervisor if not null, only return WorkerSummaries for that supervisor
     * @param owner            owner of the topology
     */
    public static List<WorkerSummary> aggWorkerStats(String stormId, String stormName,
                                                     Map<Integer, String> task2Component,
                                                     Map<List<Integer>, Map<String, Object>> beats,
                                                     Map<List<Long>, List<Object>> exec2NodePort,
                                                     Map<String, String> nodeHost,
                                                     Map<WorkerSlot, WorkerResources> worker2Resources,
                                                     boolean includeSys,
                                                     boolean userAuthorized,
                                                     String filterSupervisor,
                                                     String owner) {

        // host,port => WorkerSummary
        HashMap<WorkerSlot, WorkerSummary> workerSummaryMap = new HashMap<>();

        if (exec2NodePort != null) {
            // for each executor -> node+port pair
            for (Map.Entry<List<Long>, List<Object>> execNodePort : exec2NodePort.entrySet()) {
                List<Object> nodePort = execNodePort.getValue();
                String node = (String) nodePort.get(0);
                Long port = (Long) nodePort.get(1);
                String host = nodeHost.get(node);
                WorkerSlot slot = new WorkerSlot(node, port);
                WorkerResources resources = worker2Resources.get(slot);

                if (filterSupervisor == null || node.equals(filterSupervisor)) {
                    WorkerSummary ws = workerSummaryMap.get(slot);

                    if (ws == null) {
                        ws = new WorkerSummary();
                        ws.set_host(host);
                        ws.set_port(port.intValue());
                        ws.set_supervisor_id(node);
                        ws.set_topology_id(stormId);
                        ws.set_topology_name(stormName);
                        ws.set_num_executors(0);
                        ws.set_owner(owner);
                        if (resources != null) {
                            ws.set_assigned_memonheap(resources.get_mem_on_heap());
                            ws.set_assigned_memoffheap(resources.get_mem_off_heap());
                            ws.set_assigned_cpu(resources.get_cpu());
                        } else {
                            ws.set_assigned_memonheap(0);
                            ws.set_assigned_memoffheap(0);
                            ws.set_assigned_cpu(0);
                        }
                        ws.set_component_to_num_tasks(new HashMap<String, Long>());
                        workerSummaryMap.put(slot, ws);
                    }
                    Map<String, Long> componentToNumTasks = ws.get_component_to_num_tasks();

                    // gets min/max task pairs (executors): [1 1] [2 3] ...
                    List<Long> exec = execNodePort.getKey();
                    // get executor heartbeat
                    int hbeatSecs = 0;
                    if (beats != null) {
                        Map<String, Object> beat = beats.get(ClientStatsUtil.convertExecutor(exec));
                        hbeatSecs = beat == null ? 0 : (int) beat.get("uptime");
                    }
                    ws.set_uptime_secs(hbeatSecs);
                    ws.set_num_executors(ws.get_num_executors() + 1);

                    // get tasks if the user is authorized for this topology
                    if (userAuthorized) {
                        int firstTask = exec.get(0).intValue();
                        int lastTask = exec.get(1).intValue();

                        // get per task components
                        for (int task = firstTask; task <= lastTask; task++) {
                            String component = task2Component.get(task);
                            // if the component is a system (__*) component and we are hiding
                            // them in UI, keep going
                            if (!includeSys && Utils.isSystemId(component)) {
                                continue;
                            }

                            // good to go, increment # of tasks this component is being executed on
                            Long counter = componentToNumTasks.get(component);
                            if (counter == null) {
                                counter = new Long(0);
                            }
                            componentToNumTasks.put(component, counter + 1);
                        }
                    }
                }
            }
        }
        return new ArrayList<WorkerSummary>(workerSummaryMap.values());
    }

    // =====================================================================================
    // convert thrift stats to java maps
    // =====================================================================================

    /**
     * convert thrift executor heartbeats into a java HashMap.
     */
    public static Map<List<Integer>, Map<String, Object>> convertExecutorBeats(Map<ExecutorInfo, ExecutorBeat> beats) {
        Map<List<Integer>, Map<String, Object>> ret = new HashMap<>();
        for (Map.Entry<ExecutorInfo, ExecutorBeat> beat : beats.entrySet()) {
            ExecutorInfo executorInfo = beat.getKey();
            ExecutorBeat executorBeat = beat.getValue();
            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    convertZkExecutorHb(executorBeat));
        }

        return ret;
    }

    /**
     * convert {@link SupervisorWorkerHeartbeat} to nimbus local report executor heartbeats.
     */
    public static Map<List<Integer>, Map<String, Object>> convertWorkerBeats(SupervisorWorkerHeartbeat workerHeartbeat) {
        Map<List<Integer>, Map<String, Object>> ret = new HashMap<>();
        for (ExecutorInfo executorInfo : workerHeartbeat.get_executors()) {
            Map<String, Object> reportBeat = new HashMap<>();
            reportBeat.put(ClientStatsUtil.TIME_SECS, workerHeartbeat.get_time_secs());
            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    reportBeat);
        }

        return ret;
    }

    /**
     * convert thrift ExecutorBeat into a java HashMap.
     */
    public static Map<String, Object> convertZkExecutorHb(ExecutorBeat beat) {
        Map<String, Object> ret = new HashMap<>();
        if (beat != null) {
            ret.put(ClientStatsUtil.TIME_SECS, beat.getTimeSecs());
            ret.put(ClientStatsUtil.UPTIME, beat.getUptime());
            ret.put(STATS, convertExecutorStats(beat.getStats()));
        }

        return ret;
    }

    /**
     * convert a thrift worker heartbeat into a java HashMap.
     */
    public static Map<String, Object> convertZkWorkerHb(ClusterWorkerHeartbeat workerHb) {
        Map<String, Object> ret = new HashMap<>();
        if (workerHb != null) {
            ret.put("storm-id", workerHb.get_storm_id());
            ret.put(ClientStatsUtil.EXECUTOR_STATS, convertExecutorsStats(workerHb.get_executor_stats()));
            ret.put(ClientStatsUtil.UPTIME, workerHb.get_uptime_secs());
            ret.put(ClientStatsUtil.TIME_SECS, workerHb.get_time_secs());
        }
        return ret;
    }

    /**
     * convert executors stats into a HashMap, note that ExecutorStats are remained unchanged.
     */
    public static Map<List<Integer>, ExecutorStats> convertExecutorsStats(Map<ExecutorInfo, ExecutorStats> stats) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Map.Entry<ExecutorInfo, ExecutorStats> entry : stats.entrySet()) {
            ExecutorInfo executorInfo = entry.getKey();
            ExecutorStats executorStats = entry.getValue();

            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    executorStats);
        }
        return ret;
    }

    /**
     * convert thrift ExecutorStats structure into a java HashMap.
     */
    public static Map<String, Object> convertExecutorStats(ExecutorStats stats) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(EMITTED, stats.get_emitted());
        ret.put(TRANSFERRED, stats.get_transferred());
        ret.put(RATE, stats.get_rate());

        if (stats.get_specific().is_set_bolt()) {
            ret.putAll(convertSpecificStats(stats.get_specific().get_bolt()));
            ret.put(TYPE, ClientStatsUtil.BOLT);
        } else {
            ret.putAll(convertSpecificStats(stats.get_specific().get_spout()));
            ret.put(TYPE, ClientStatsUtil.SPOUT);
        }

        return ret;
    }

    private static Map<String, Object> convertSpecificStats(SpoutStats stats) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(ACKED, stats.get_acked());
        ret.put(FAILED, stats.get_failed());
        ret.put(COMP_LATENCIES, stats.get_complete_ms_avg());

        return ret;
    }

    private static Map<String, Object> convertSpecificStats(BoltStats stats) {
        Map<String, Object> ret = new HashMap<>();

        Map acked = ClientStatsUtil.windowSetConverter(stats.get_acked(), FROM_GSID, ClientStatsUtil.IDENTITY);
        Map failed = ClientStatsUtil.windowSetConverter(stats.get_failed(), FROM_GSID, ClientStatsUtil.IDENTITY);
        Map processAvg = ClientStatsUtil.windowSetConverter(stats.get_process_ms_avg(), FROM_GSID, ClientStatsUtil.IDENTITY);
        Map executed = ClientStatsUtil.windowSetConverter(stats.get_executed(), FROM_GSID, ClientStatsUtil.IDENTITY);
        Map executeAvg = ClientStatsUtil.windowSetConverter(stats.get_execute_ms_avg(), FROM_GSID, ClientStatsUtil.IDENTITY);

        ret.put(ACKED, acked);
        ret.put(FAILED, failed);
        ret.put(PROC_LATENCIES, processAvg);
        ret.put(EXECUTED, executed);
        ret.put(EXEC_LATENCIES, executeAvg);

        return ret;
    }

    /**
     * extract a list of host port info for specified component.
     *
     * @param exec2hostPort  {executor -> host+port}
     * @param task2component {task id -> component}
     * @param includeSys     whether to include system streams
     * @param compId         component id
     * @return a list of host+port
     */
    public static List<Map<String, Object>> extractNodeInfosFromHbForComp(
        Map<List<? extends Number>, List<Object>> exec2hostPort, Map<Integer, String> task2component, boolean includeSys, String compId) {
        List<Map<String, Object>> ret = new ArrayList<>();

        Set<List> hostPorts = new HashSet<>();
        for (Entry<List<? extends Number>, List<Object>> entry : exec2hostPort.entrySet()) {
            List<? extends Number> key = entry.getKey();
            List<Object> value = entry.getValue();

            Integer start = key.get(0).intValue();
            String host = (String) value.get(0);
            Integer port = (Integer) value.get(1);
            String comp = task2component.get(start);
            if ((compId == null || compId.equals(comp)) && (includeSys || !Utils.isSystemId(comp))) {
                hostPorts.add(Lists.newArrayList(host, port));
            }
        }

        for (List hostPort : hostPorts) {
            Map<String, Object> m = new HashMap<>();
            m.put(HOST, hostPort.get(0));
            m.put(PORT, hostPort.get(1));
            ret.add(m);
        }

        return ret;
    }


    // =====================================================================================
    // heartbeats related
    // =====================================================================================

    /**
     * extracts a list of executor data from heart beats.
     */
    public static List<Map<String, Object>> extractDataFromHb(Map executor2hostPort, Map task2component,
                                                              Map<List<Integer>, Map<String, Object>> beats,
                                                              boolean includeSys, StormTopology topology) {
        return extractDataFromHb(executor2hostPort, task2component, beats, includeSys, topology, null);
    }

    /**
     * extracts a list of executor data from heart beats.
     */
    public static List<Map<String, Object>> extractDataFromHb(Map executor2hostPort, Map task2component,
                                                              Map<List<Integer>, Map<String, Object>> beats,
                                                              boolean includeSys, StormTopology topology, String compId) {
        List<Map<String, Object>> ret = new ArrayList<>();
        if (executor2hostPort == null || beats == null) {
            return ret;
        }
        for (Object o : executor2hostPort.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            List executor = (List) entry.getKey();
            List hostPort = (List) entry.getValue();

            Integer start = ((Number) executor.get(0)).intValue();
            Integer end = ((Number) executor.get(1)).intValue();

            String host = (String) hostPort.get(0);
            Integer port = ((Number) hostPort.get(1)).intValue();

            Map<String, Object> beat = beats.get(ClientStatsUtil.convertExecutor(executor));
            if (beat == null) {
                continue;
            }
            String id = (String) task2component.get(start);

            Map<String, Object> m = new HashMap<>();
            if ((compId == null || compId.equals(id)) && (includeSys || !Utils.isSystemId(id))) {
                m.put("exec-id", entry.getKey());
                m.put("comp-id", id);
                m.put(NUM_TASKS, end - start + 1);
                m.put(HOST, host);
                m.put(PORT, port);

                Map stats = ClientStatsUtil.getMapByKey(beat, STATS);
                m.put(ClientStatsUtil.UPTIME, beat.get(ClientStatsUtil.UPTIME));
                m.put(STATS, stats);

                String type = componentType(topology, compId);
                if (type != null) {
                    m.put(TYPE, type);
                } else {
                    m.put(TYPE, stats.get(TYPE));
                }
                ret.add(m);
            }
        }
        return ret;
    }

    /**
     * compute weighted avg from a Map of stats and given avg/count keys.
     *
     * @param accData    a Map of {win -> key -> value}
     * @param wgtAvgKey  weighted average key
     * @param divisorKey count key
     * @return a Map of {win -> weighted avg value}
     */
    private static Map<String, Double> computeWeightedAveragesPerWindow(Map<String, Object> accData,
                                                                        String wgtAvgKey, String divisorKey) {
        Map<String, Double> ret = new HashMap<>();
        for (Object o : ClientStatsUtil.getMapByKey(accData, wgtAvgKey).entrySet()) {
            Map.Entry e = (Map.Entry) o;
            Object window = e.getKey();
            double wgtAvg = ((Number) e.getValue()).doubleValue();
            long divisor = ((Number) ClientStatsUtil.getMapByKey(accData, divisorKey).get(window)).longValue();
            if (divisor > 0) {
                ret.put(window.toString(), wgtAvg / divisor);
            }
        }
        return ret;
    }

    /**
     * computes max bolt capacity.
     *
     * @param executorSumms a list of ExecutorSummary
     * @return max bolt capacity
     */
    public static double computeBoltCapacity(List<ExecutorSummary> executorSumms) {
        double max = 0.0;
        for (ExecutorSummary summary : executorSumms) {
            double capacity = computeExecutorCapacity(summary);
            if (capacity > max) {
                max = capacity;
            }
        }
        return max;
    }

    /**
     * Compute the capacity of a executor. approximation of the % of time spent doing real work.
     * @param summary the stats for the executor.
     * @return the capacity of the executor.
     */
    public static double computeExecutorCapacity(ExecutorSummary summary) {
        ExecutorStats stats = summary.get_stats();
        if (stats == null) {
            return 0.0;
        } else {
            // actual value of m is: Map<String, Map<String/GlobalStreamId, Long/Double>> ({win -> stream -> value})
            Map<String, Map> m = aggregateBoltStats(Lists.newArrayList(summary), true);
            // {metric -> win -> value} ==> {win -> metric -> value}
            m = swapMapOrder(aggregateBoltStreams(m));
            // {metric -> value}
            Map data = ClientStatsUtil.getMapByKey(m, TEN_MIN_IN_SECONDS_STR);

            int uptime = summary.get_uptime_secs();
            int win = Math.min(uptime, TEN_MIN_IN_SECONDS);
            long executed = getByKeyOr0(data, EXECUTED).longValue();
            double latency = getByKeyOr0(data, EXEC_LATENCIES).doubleValue();
            if (win > 0) {
                return executed * latency / (1000 * win);
            }
            return 0.0;
        }
    }

    /**
     * filter ExecutorSummary whose stats is null.
     *
     * @param summs a list of ExecutorSummary
     * @return filtered summs
     */
    public static List<ExecutorSummary> getFilledStats(List<ExecutorSummary> summs) {
        List<ExecutorSummary> ret = new ArrayList<>();
        for (ExecutorSummary summ : summs) {
            if (summ.get_stats() != null) {
                ret.add(summ);
            }
        }
        return ret;
    }

    private static <K, V> Map<String, V> mapKeyStr(Map<K, V> m) {
        Map<String, V> ret = new HashMap<>();
        for (Map.Entry<K, V> entry : m.entrySet()) {
            ret.put(entry.getKey().toString(), entry.getValue());
        }
        return ret;
    }

    private static <K1, K2> long sumStreamsLong(Map<K1, Map<K2, ?>> m, String key) {
        long sum = 0;
        if (m == null) {
            return sum;
        }
        for (Map<K2, ?> v : m.values()) {
            for (Map.Entry<K2, ?> entry : v.entrySet()) {
                if (entry.getKey().equals(key)) {
                    sum += ((Number) entry.getValue()).longValue();
                }
            }
        }
        return sum;
    }

    private static <K1, K2> double sumStreamsDouble(Map<K1, Map<K2, ?>> m, String key) {
        double sum = 0;
        if (m == null) {
            return sum;
        }
        for (Map<K2, ?> v : m.values()) {
            for (Map.Entry<K2, ?> entry : v.entrySet()) {
                if (entry.getKey().equals(key)) {
                    sum += ((Number) entry.getValue()).doubleValue();
                }
            }
        }
        return sum;
    }

    /**
     * same as clojure's (merge-with merge m1 m2).
     */
    private static Map mergeMaps(Map m1, Map m2) {
        if (m2 == null) {
            return m1;
        }
        for (Object o : m2.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object k = entry.getKey();

            Map existing = (Map) m1.get(k);
            if (existing == null) {
                m1.put(k, entry.getValue());
            } else {
                existing.putAll((Map) m2.get(k));
            }
        }
        return m1;
    }


    /**
     * filter system streams from stats.
     *
     * @param stream2stat { stream id -> value }
     * @param includeSys  whether to filter system streams
     * @return filtered stats
     */
    private static <K, V> Map<K, V> filterSysStreams2Stat(Map<K, V> stream2stat, boolean includeSys) {
        LOG.trace("Filter Sys Streams2Stat {}", stream2stat);
        if (!includeSys) {
            for (Iterator itr = stream2stat.keySet().iterator(); itr.hasNext(); ) {
                Object key = itr.next();
                if (key instanceof String && Utils.isSystemId((String) key)) {
                    itr.remove();
                }
            }
        }
        return stream2stat;
    }

    /**
     * filter system streams from stats.
     *
     * @param stats      { win -> stream id -> value }
     * @param includeSys whether to filter system streams
     * @return filtered stats
     */
    private static <K, V> Map<String, Map<K, V>> filterSysStreams(Map<String, Map<K, V>> stats, boolean includeSys) {
        LOG.trace("Filter Sys Streams {}", stats);
        if (!includeSys) {
            for (Iterator<String> itr = stats.keySet().iterator(); itr.hasNext(); ) {
                String winOrStream = itr.next();
                Map<K, V> stream2stat = stats.get(winOrStream);
                for (Iterator subItr = stream2stat.keySet().iterator(); subItr.hasNext(); ) {
                    Object key = subItr.next();
                    if (key instanceof String && Utils.isSystemId((String) key)) {
                        subItr.remove();
                    }
                }
            }
        }
        return stats;
    }

    /**
     * equals to clojure's: (merge-with (partial merge-with sum-or-0) acc-out spout-out).
     */
    private static <K1, K2> Map<K1, Map<K2, Number>> fullMergeWithSum(Map<K1, Map<K2, ?>> m1,
                                                                      Map<K1, Map<K2, ?>> m2) {
        Set<K1> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        Map<K1, Map<K2, Number>> ret = new HashMap<>();
        for (K1 k : allKeys) {
            Map<K2, ?> mm1 = null;
            Map<K2, ?> mm2 = null;
            if (m1 != null) {
                mm1 = m1.get(k);
            }
            if (m2 != null) {
                mm2 = m2.get(k);
            }
            ret.put(k, mergeWithSum(mm1, mm2));
        }

        return ret;
    }

    private static <K> Map<K, Number> mergeWithSum(Map<K, ?> m1, Map<K, ?> m2) {
        Map<K, Number> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            if (n1 instanceof Long) {
                ret.put(k, n1.longValue() + n2.longValue());
            } else {
                ret.put(k, n1.doubleValue() + n2.doubleValue());
            }
        }
        return ret;
    }

    private static <K> Map<K, Long> mergeWithSumLong(Map<K, Long> m1, Map<K, Long> m2) {
        Map<K, Long> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            ret.put(k, n1.longValue() + n2.longValue());
        }
        return ret;
    }

    private static <K> Map<K, Double> mergeWithSumDouble(Map<K, Double> m1, Map<K, Double> m2) {
        Map<K, Double> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            ret.put(k, n1.doubleValue() + n2.doubleValue());
        }
        return ret;
    }

    /**
     * this method merges 2 two-level-deep maps.
     */
    private static <K> Map<String, Map<K, List>> mergeWithAddPair(Map<String, Map<K, List>> m1,
                                                                  Map<String, Map<K, List>> m2) {
        Map<String, Map<K, List>> ret = new HashMap<>();

        Set<String> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (String k : allKeys) {
            Map<K, List> mm1 = (m1 != null) ? m1.get(k) : null;
            Map<K, List> mm2 = (m2 != null) ? m2.get(k) : null;
            if (mm1 == null && mm2 == null) {
                continue;
            } else if (mm1 == null) {
                ret.put(k, mm2);
            } else if (mm2 == null) {
                ret.put(k, mm1);
            } else {
                Map<K, List> tmp = new HashMap<>();
                for (K kk : mm1.keySet()) {
                    List seq1 = mm1.get(kk);
                    List seq2 = mm2.get(kk);
                    if (seq1 == null && seq2 == null) {
                        continue;
                    } else if (seq1 == null) {
                        tmp.put(kk, seq2);
                    } else if (seq2 == null) {
                        tmp.put(kk, seq1);
                    } else {
                        List sums = new ArrayList();
                        for (int i = 0; i < seq1.size(); i++) {
                            if (seq1.get(i) instanceof Long) {
                                sums.add(((Number) seq1.get(i)).longValue() + ((Number) seq2.get(i)).longValue());
                            } else {
                                sums.add(((Number) seq1.get(i)).doubleValue() + ((Number) seq2.get(i)).doubleValue());
                            }
                        }
                        tmp.put(kk, sums);
                    }
                }
                ret.put(k, tmp);
            }
        }
        return ret;
    }

    // =====================================================================================
    // thriftify stats methods
    // =====================================================================================

    /**
     * Used for local test.
     */
    public static SupervisorWorkerHeartbeat thriftifyRpcWorkerHb(String stormId, List<Long> executorId) {
        SupervisorWorkerHeartbeat supervisorWorkerHeartbeat = new SupervisorWorkerHeartbeat();
        supervisorWorkerHeartbeat.set_storm_id(stormId);
        supervisorWorkerHeartbeat
            .set_executors(Collections.singletonList(new ExecutorInfo(executorId.get(0).intValue(), executorId.get(1).intValue())));
        supervisorWorkerHeartbeat.set_time_secs(Time.currentTimeSecs());
        return supervisorWorkerHeartbeat;
    }

    private static ComponentAggregateStats thriftifySpoutAggStats(Map m) {
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.SPOUT);
        stats.set_last_error((ErrorInfo) m.get(LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        SpoutAggregateStats spoutAggStats = new SpoutAggregateStats();
        spoutAggStats.set_complete_latency_ms(getByKeyOr0(m, COMP_LATENCY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.spout(spoutAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ComponentAggregateStats thriftifyBoltAggStats(Map m) {
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.BOLT);
        stats.set_last_error((ErrorInfo) m.get(LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        BoltAggregateStats boltAggStats = new BoltAggregateStats();
        boltAggStats.set_execute_latency_ms(getByKeyOr0(m, EXEC_LATENCY).doubleValue());
        boltAggStats.set_process_latency_ms(getByKeyOr0(m, PROC_LATENCY).doubleValue());
        boltAggStats.set_executed(getByKeyOr0(m, EXECUTED).longValue());
        boltAggStats.set_capacity(getByKeyOr0(m, CAPACITY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.bolt(boltAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ExecutorAggregateStats thriftifyExecAggStats(String compId, String compType, Map m) {
        ExecutorSummary executorSummary = new ExecutorSummary();
        List executor = (List) m.get(EXECUTOR_ID);
        executorSummary.set_executor_info(new ExecutorInfo(((Number) executor.get(0)).intValue(),
                                                           ((Number) executor.get(1)).intValue()));
        executorSummary.set_component_id(compId);
        executorSummary.set_host((String) m.get(HOST));
        executorSummary.set_port(getByKeyOr0(m, PORT).intValue());
        int uptime = getByKeyOr0(m, ClientStatsUtil.UPTIME).intValue();
        executorSummary.set_uptime_secs(uptime);
        ExecutorAggregateStats stats = new ExecutorAggregateStats();
        stats.set_exec_summary(executorSummary);

        if (compType.equals(ClientStatsUtil.SPOUT)) {
            stats.set_stats(thriftifySpoutAggStats(m));
        } else {
            stats.set_stats(thriftifyBoltAggStats(m));
        }

        return stats;
    }

    private static Map thriftifyBoltOutputStats(Map id2outStats) {
        Map ret = new HashMap();
        for (Object k : id2outStats.keySet()) {
            ret.put(k, thriftifyBoltAggStats((Map) id2outStats.get(k)));
        }
        return ret;
    }

    private static Map thriftifySpoutOutputStats(Map id2outStats) {
        Map ret = new HashMap();
        for (Object k : id2outStats.keySet()) {
            ret.put(k, thriftifySpoutAggStats((Map) id2outStats.get(k)));
        }
        return ret;
    }

    private static Map thriftifyBoltInputStats(Map cidSid2inputStats) {
        Map ret = new HashMap();
        for (Object e : cidSid2inputStats.entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            ret.put(toGlobalStreamId((List) entry.getKey()),
                    thriftifyBoltAggStats((Map) entry.getValue()));
        }
        return ret;
    }

    private static ComponentAggregateStats thriftifyCommonAggStats(ComponentAggregateStats stats, Map m) {
        CommonAggregateStats commonStats = new CommonAggregateStats();
        commonStats.set_num_tasks(getByKeyOr0(m, NUM_TASKS).intValue());
        commonStats.set_num_executors(getByKeyOr0(m, NUM_EXECUTORS).intValue());
        commonStats.set_emitted(getByKeyOr0(m, EMITTED).longValue());
        commonStats.set_transferred(getByKeyOr0(m, TRANSFERRED).longValue());
        commonStats.set_acked(getByKeyOr0(m, ACKED).longValue());
        commonStats.set_failed(getByKeyOr0(m, FAILED).longValue());

        stats.set_common_stats(commonStats);
        return stats;
    }

    private static ComponentPageInfo thriftifyCompPageData(
        String topologyId, StormTopology topology, String compId, Map<String, Object> data) {
        ComponentPageInfo ret = new ComponentPageInfo();
        ret.set_component_id(compId);

        Map win2stats = new HashMap();
        win2stats.put(EMITTED, ClientStatsUtil.getMapByKey(data, WIN_TO_EMITTED));
        win2stats.put(TRANSFERRED, ClientStatsUtil.getMapByKey(data, WIN_TO_TRANSFERRED));
        win2stats.put(ACKED, ClientStatsUtil.getMapByKey(data, WIN_TO_ACKED));
        win2stats.put(FAILED, ClientStatsUtil.getMapByKey(data, WIN_TO_FAILED));

        String compType = (String) data.get(TYPE);
        if (compType.equals(ClientStatsUtil.SPOUT)) {
            ret.set_component_type(ComponentType.SPOUT);
            win2stats.put(COMP_LATENCY, ClientStatsUtil.getMapByKey(data, WIN_TO_COMP_LAT));
        } else {
            ret.set_component_type(ComponentType.BOLT);
            win2stats.put(EXEC_LATENCY, ClientStatsUtil.getMapByKey(data, WIN_TO_EXEC_LAT));
            win2stats.put(PROC_LATENCY, ClientStatsUtil.getMapByKey(data, WIN_TO_PROC_LAT));
            win2stats.put(EXECUTED, ClientStatsUtil.getMapByKey(data, WIN_TO_EXECUTED));
        }
        win2stats = swapMapOrder(win2stats);

        List<ExecutorAggregateStats> execStats = new ArrayList<>();
        List executorStats = (List) data.get(ClientStatsUtil.EXECUTOR_STATS);
        if (executorStats != null) {
            for (Object o : executorStats) {
                execStats.add(thriftifyExecAggStats(compId, compType, (Map) o));
            }
        }

        Map gsid2inputStats;
        Map sid2outputStats;
        if (compType.equals(ClientStatsUtil.SPOUT)) {
            Map tmp = new HashMap();
            for (Object k : win2stats.keySet()) {
                tmp.put(k, thriftifySpoutAggStats((Map) win2stats.get(k)));
            }
            win2stats = tmp;
            gsid2inputStats = null;
            sid2outputStats = thriftifySpoutOutputStats(ClientStatsUtil.getMapByKey(data, SID_TO_OUT_STATS));
        } else {
            Map tmp = new HashMap();
            for (Object k : win2stats.keySet()) {
                tmp.put(k, thriftifyBoltAggStats((Map) win2stats.get(k)));
            }
            win2stats = tmp;
            gsid2inputStats = thriftifyBoltInputStats(ClientStatsUtil.getMapByKey(data, CID_SID_TO_IN_STATS));
            sid2outputStats = thriftifyBoltOutputStats(ClientStatsUtil.getMapByKey(data, SID_TO_OUT_STATS));
        }
        ret.set_num_executors(getByKeyOr0(data, NUM_EXECUTORS).intValue());
        ret.set_num_tasks(getByKeyOr0(data, NUM_TASKS).intValue());
        ret.set_topology_id(topologyId);
        ret.set_topology_name(null);
        ret.set_window_to_stats(win2stats);
        ret.set_sid_to_output_stats(sid2outputStats);
        ret.set_exec_stats(execStats);
        ret.set_gsid_to_input_stats(gsid2inputStats);

        return ret;
    }

    /**
     * Convert Executor stats to thrift data structure.
     * @param stats the stats in the form of a map.
     * @return teh thrift structure for the stats.
     */
    public static ExecutorStats thriftifyExecutorStats(Map stats) {
        ExecutorStats ret = new ExecutorStats();
        ExecutorSpecificStats specificStats = thriftifySpecificStats(stats);
        ret.set_specific(specificStats);

        ret.set_emitted(ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, EMITTED), TO_STRING, TO_STRING));
        ret.set_transferred(ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, TRANSFERRED), TO_STRING, TO_STRING));
        ret.set_rate(((Number) stats.get(RATE)).doubleValue());

        return ret;
    }

    private static ExecutorSpecificStats thriftifySpecificStats(Map stats) {
        ExecutorSpecificStats specificStats = new ExecutorSpecificStats();

        String compType = (String) stats.get(TYPE);
        if (ClientStatsUtil.BOLT.equals(compType)) {
            BoltStats boltStats = new BoltStats();
            boltStats.set_acked(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, ACKED), ClientStatsUtil.TO_GSID, TO_STRING));
            boltStats.set_executed(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, EXECUTED), ClientStatsUtil.TO_GSID, TO_STRING));
            boltStats.set_execute_ms_avg(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, EXEC_LATENCIES), ClientStatsUtil.TO_GSID, TO_STRING));
            boltStats.set_failed(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, FAILED), ClientStatsUtil.TO_GSID, TO_STRING));
            boltStats.set_process_ms_avg(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, PROC_LATENCIES), ClientStatsUtil.TO_GSID, TO_STRING));
            specificStats.set_bolt(boltStats);
        } else {
            SpoutStats spoutStats = new SpoutStats();
            spoutStats.set_acked(ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, ACKED), TO_STRING, TO_STRING));
            spoutStats.set_failed(ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, FAILED), TO_STRING, TO_STRING));
            spoutStats.set_complete_ms_avg(
                ClientStatsUtil.windowSetConverter(ClientStatsUtil.getMapByKey(stats, COMP_LATENCIES), TO_STRING, TO_STRING));
            specificStats.set_spout(spoutStats);
        }
        return specificStats;
    }


    // =====================================================================================
    // helper methods
    // =====================================================================================

    private static GlobalStreamId toGlobalStreamId(List list) {
        return new GlobalStreamId((String) list.get(0), (String) list.get(1));
    }

    /**
     * Returns true if x is a number that is not NaN or Infinity, false otherwise.
     */
    private static boolean isValidNumber(Object x) {
        return x != null && x instanceof Number
            && !Double.isNaN(((Number) x).doubleValue())
            && !Double.isInfinite(((Number) x).doubleValue());
    }

    /*
     * the value of m is as follows:
     * <pre>
     * #org.apache.storm.stats.CommonStats {
     *  :executed {
     *      ":all-time" {["split" "default"] 18727460},
     *      "600" {["split" "default"] 11554},
     *      "10800" {["split" "default"] 207269},
     *      "86400" {["split" "default"] 1659614}},
     *  :execute-latencies {
     *      ":all-time" {["split" "default"] 0.5874528633354443},
     *      "600" {["split" "default"] 0.6140350877192983},
     *      "10800" {["split" "default"] 0.5864434687156971},
     *      "86400" {["split" "default"] 0.5815376460556336}}
     * }
     * </pre>
     */
    private static double computeAggCapacity(Map m, Integer uptime) {
        if (uptime != null && uptime != 0) {
            Map execAvg = (Map) ((Map) m.get(EXEC_LATENCIES)).get(TEN_MIN_IN_SECONDS_STR);
            Map exec = (Map) ((Map) m.get(EXECUTED)).get(TEN_MIN_IN_SECONDS_STR);

            Set<Object> allKeys = new HashSet<>();
            if (execAvg != null) {
                allKeys.addAll(execAvg.keySet());
            }
            if (exec != null) {
                allKeys.addAll(exec.keySet());
            }

            double totalAvg = 0;
            for (Object k : allKeys) {
                double avg = getOr0(execAvg, k).doubleValue();
                long cnt = getOr0(exec, k).longValue();
                totalAvg += avg * cnt;
            }
            return totalAvg / (Math.min(uptime, TEN_MIN_IN_SECONDS) * 1000);
        }
        return 0.0;
    }

    private static Number getOr0(Map m, Object k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    private static Number getByKeyOr0(Map m, String k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    private static <T, V1 extends Number, V2 extends Number> Double weightAvgAndSum(Map<T, V1> id2Avg, Map<T, V2> id2num) {
        double ret = 0;
        if (id2Avg == null || id2num == null) {
            return ret;
        }

        for (Map.Entry<T, V1> entry : id2Avg.entrySet()) {
            T k = entry.getKey();
            ret += productOr0(entry.getValue(), id2num.get(k));
        }
        return ret;
    }

    private static <K, V1 extends Number, V2 extends Number> double weightAvg(Map<K, V1> id2Avg, Map<K, V2> id2num, K key) {
        if (id2Avg == null || id2num == null) {
            return 0.0;
        }
        return productOr0(id2Avg.get(key), id2num.get(key));
    }

    /**
     * Get the coponenet type for a give id.
     * @param topology the topology this is a part of.
     * @param compId the id of the component.
     * @return the type as a String "BOLT" or "SPOUT".
     */
    public static String componentType(StormTopology topology, String compId) {
        if (compId == null) {
            return null;
        }

        Map<String, Bolt> bolts = topology.get_bolts();
        if (Utils.isSystemId(compId) || bolts.containsKey(compId)) {
            return ClientStatsUtil.BOLT;
        }
        return ClientStatsUtil.SPOUT;
    }

    private static <K, V extends Number> long sumValues(Map<K, V> m) {
        long ret = 0L;
        if (m == null) {
            return ret;
        }

        for (Number n : m.values()) {
            ret += n.longValue();
        }
        return ret;
    }

    private static Number sumOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            if (a instanceof Long || a instanceof Integer) {
                return ((Number) a).longValue() + ((Number) b).longValue();
            } else {
                return ((Number) a).doubleValue() + ((Number) b).doubleValue();
            }
        }
        return 0;
    }

    private static double productOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        }
        return 0;
    }

    private static double maxOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            return Math.max(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return 0;
    }

    /**
     * For a nested map, rearrange data such that the top-level keys become the nested map's keys and vice versa. Example: {:a {:X :banana,
     * :Y :pear}, :b {:X :apple, :Y :orange}} -> {:Y {:a :pear, :b :orange}, :X {:a :banana, :b :apple}}"
     */
    private static Map swapMapOrder(Map m) {
        if (m.size() == 0) {
            return m;
        }

        Map ret = new HashMap();
        for (Object k1 : m.keySet()) {
            Map v = (Map) m.get(k1);
            if (v != null) {
                for (Object k2 : v.keySet()) {
                    Map subRet = (Map) ret.get(k2);
                    if (subRet == null) {
                        subRet = new HashMap();
                        ret.put(k2, subRet);
                    }
                    subRet.put(k1, v.get(k2));
                }
            }
        }
        return ret;
    }

    /**
     * Expand the count/average out into total, count.
     * @param avgs   a HashMap of values: { win -> GlobalStreamId -> value }
     * @param counts a HashMap of values: { win -> GlobalStreamId -> value }
     * @return a HashMap of values: {win -> GlobalStreamId -> [cnt*avg, cnt]}
     */
    private static <K> Map<String, Map<K, List>> expandAverages(Map<String, Map<K, Double>> avgs,
                                                                Map<String, Map<K, Long>> counts) {
        Map<String, Map<K, List>> ret = new HashMap<>();

        for (String win : counts.keySet()) {
            Map<K, List> inner = new HashMap<>();

            Map<K, Long> stream2cnt = counts.get(win);
            for (K stream : stream2cnt.keySet()) {
                Long cnt = stream2cnt.get(stream);
                Double avg = avgs.get(win).get(stream);
                if (avg == null) {
                    avg = 0.0;
                }
                inner.put(stream, Lists.newArrayList(cnt * avg, cnt));
            }
            ret.put(win, inner);
        }

        return ret;
    }

    /**
     * first zip the two seqs, then do expand-average, then merge with sum.
     *
     * @param avgSeq   list of avgs like: [{win -> GlobalStreamId -> value}, ...]
     * @param countSeq list of counts like [{win -> GlobalStreamId -> value}, ...]
     */
    private static <K> Map<String, Map<K, List>> expandAveragesSeq(
        List<Map<String, Map<K, Double>>> avgSeq, List<Map<String, Map<K, Long>>> countSeq) {
        Map<String, Map<K, List>> initVal = null;
        for (int i = 0; i < avgSeq.size(); i++) {
            Map<String, Map<K, Double>> avg = avgSeq.get(i);
            Map<String, Map<K, Long>> count = (Map) countSeq.get(i);
            if (initVal == null) {
                initVal = expandAverages(avg, count);
            } else {
                initVal = mergeWithAddPair(initVal, expandAverages(avg, count));
            }
        }
        if (initVal == null) {
            initVal = new HashMap<>();
        }
        return initVal;
    }

    private static double valAvg(double t, long c) {
        if (c == 0) {
            return 0;
        }
        return t / c;
    }

    /**
     * Convert a float to a string for display.
     * @param n the value to format.
     * @return the string ready for display.
     */
    public static String floatStr(Double n) {
        if (n == null) {
            return "0";
        }
        return String.format("%.3f", n);
    }

    public static String errorSubset(String errorStr) {
        return errorStr.substring(0, 200);
    }

    private static ErrorInfo getLastError(IStormClusterState stormClusterState, String stormId, String compId) {
        return stormClusterState.lastError(stormId, compId);
    }


    // =====================================================================================
    // key transformers
    // =====================================================================================

    public static <K> Map windowSetConverter(Map stats, ClientStatsUtil.KeyTransformer<K> firstKeyFunc) {
        return ClientStatsUtil.windowSetConverter(stats, ClientStatsUtil.IDENTITY, firstKeyFunc);
    }

    static class FromGlobalStreamIdTransformer implements ClientStatsUtil.KeyTransformer<List<String>> {
        @Override
        public List<String> transform(Object key) {
            GlobalStreamId sid = (GlobalStreamId) key;
            return Lists.newArrayList(sid.get_componentId(), sid.get_streamId());
        }
    }

    static class ToStringTransformer implements ClientStatsUtil.KeyTransformer<String> {
        @Override
        public String transform(Object key) {
            return key.toString();
        }
    }
}
