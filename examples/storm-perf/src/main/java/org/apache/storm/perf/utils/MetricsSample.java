/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf.utils;

import java.util.List;
import java.util.Map;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.Utils;

public class MetricsSample {

    private long sampleTime = -1L;
    private long totalTransferred = 0L;
    private long totalEmitted = 0L;
    private long totalAcked = 0L;
    private long totalFailed = 0L;

    private double totalLatency;

    private long spoutEmitted = 0L;
    private long spoutTransferred = 0L;
    private int spoutExecutors = 0;

    private int numSupervisors = 0;
    private int numWorkers = 0;
    private int numTasks = 0;
    private int numExecutors = 0;

    private int totalSlots = 0;
    private int usedSlots = 0;

    public static MetricsSample factory(Nimbus.Iface client, String topologyName) throws Exception {
        // "************ Sampling Metrics *****************

        // get topology info
        TopologySummary topSummary = client.getTopologySummaryByName(topologyName);
        int topologyExecutors = topSummary.get_num_executors();
        int topologyWorkers = topSummary.get_num_workers();
        int topologyTasks = topSummary.get_num_tasks();
        TopologyInfo topInfo = client.getTopologyInfo(topSummary.get_id());

        MetricsSample sample = getMetricsSample(topInfo);
        sample.numWorkers = topologyWorkers;
        sample.numExecutors = topologyExecutors;
        sample.numTasks = topologyTasks;
        return sample;
    }

    private static MetricsSample getMetricsSample(TopologyInfo topInfo) {
        List<ExecutorSummary> executorSummaries = topInfo.get_executors();

        // totals
        long totalTransferred = 0L;
        long totalEmitted = 0L;
        long totalAcked = 0L;
        long totalFailed = 0L;

        // number of spout executors
        int spoutExecCount = 0;
        double spoutLatencySum = 0.0;
        long spoutTransferred = 0L;

        // Executor summaries
        for (ExecutorSummary executorSummary : executorSummaries) {
            ExecutorStats executorStats = executorSummary.get_stats();
            if (executorStats == null) {
                continue;
            }

            ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
            if (executorSpecificStats == null) {
                // bail out
                continue;
            }

            // transferred totals
            Map<String, Map<String, Long>> transferred = executorStats.get_transferred();
            Map<String, Long> txMap = transferred.get(":all-time");
            if (txMap == null) {
                continue;
            }
            for (String key : txMap.keySet()) {
                // todo, ignore the master batch coordinator ?
                if (!Utils.isSystemId(key)) {
                    Long count = txMap.get(key);
                    totalTransferred += count;
                    if (executorSpecificStats.is_set_spout()) {
                        spoutTransferred += count;
                    }
                }
            }

            // we found a spout
            if (executorSpecificStats.isSet(2)) { // spout

                SpoutStats spoutStats = executorSpecificStats.get_spout();
                Map<String, Long> acked = spoutStats.get_acked().get(":all-time");
                if (acked != null) {
                    for (String key : acked.keySet()) {
                        totalAcked += acked.get(key);
                    }
                }

                Map<String, Long> failed = spoutStats.get_failed().get(":all-time");
                if (failed != null) {
                    for (String key : failed.keySet()) {
                        totalFailed += failed.get(key);
                    }
                }

                Double total = 0d;
                Map<String, Double> vals = spoutStats.get_complete_ms_avg().get(":all-time");
                if (vals != null) {
                    for (String key : vals.keySet()) {
                        total += vals.get(key);
                    }
                    Double latency = total / vals.size();
                    spoutLatencySum += latency;
                }

                spoutExecCount++;
            }


        } // end executor summary

        MetricsSample ret = new MetricsSample();
        ret.totalEmitted = totalEmitted;
        ret.totalTransferred = totalTransferred;
        ret.totalAcked = totalAcked;
        ret.totalFailed = totalFailed;
        ret.totalLatency = spoutLatencySum / spoutExecCount;

        long spoutEmitted = 0L;
        ret.spoutEmitted = spoutEmitted;
        ret.spoutTransferred = spoutTransferred;
        ret.sampleTime = System.currentTimeMillis();
        //        ret.numSupervisors = clusterSummary.get_supervisors_size();
        ret.numWorkers = 0;
        ret.numExecutors = 0;
        ret.numTasks = 0;
        ret.spoutExecutors = spoutExecCount;
        return ret;
    }

    // getters
    public long getSampleTime() {
        return sampleTime;
    }

    public long getTotalTransferred() {
        return totalTransferred;
    }

    public long getTotalEmitted() {
        return totalEmitted;
    }

    public long getTotalAcked() {
        return totalAcked;
    }

    public long getTotalFailed() {
        return totalFailed;
    }

    public double getTotalLatency() {
        return totalLatency;
    }

    public long getSpoutEmitted() {
        return spoutEmitted;
    }

    public long getSpoutTransferred() {
        return spoutTransferred;
    }

    public int getNumSupervisors() {
        return numSupervisors;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public int getTotalSlots() {
        return totalSlots;
    }

    public int getSpoutExecutors() {
        return this.spoutExecutors;
    }

    public int getNumExecutors() {
        return this.numExecutors;
    }

    public int getUsedSlots() {
        return this.usedSlots;
    }

}
