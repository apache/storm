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
package org.apache.storm.scheduler.blacklist.strategies;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DefaultBlacklistStrategy implements IBlacklistStrategy {

    private static Logger LOG = LoggerFactory.getLogger(DefaultBlacklistStrategy.class);

    private IReporter _reporter;

    private int _toleranceCount;
    private int _resumeTime;
    private int _nimbusMonitorFreqSecs;

    private TreeMap<String, Integer> blacklist;

    @Override
    public void prepare(Map conf){
        if (conf.containsKey(Config.BLACKLIST_SCHEDULER_TOLERANCE_COUNT)) {
            _toleranceCount = Utils.getInt( conf.get(Config.BLACKLIST_SCHEDULER_TOLERANCE_COUNT));
        }
        if (conf.containsKey(Config.BLACKLIST_SCHEDULER_RESUME_TIME)) {
            _resumeTime = Utils.getInt( conf.get(Config.BLACKLIST_SCHEDULER_RESUME_TIME));
        }
        String reporterClassName = conf.containsKey(Config.BLACKLIST_SCHEDULER_REPORTER) ? (String) conf.get(Config.BLACKLIST_SCHEDULER_REPORTER) : "org.apache.storm.scheduler.blacklist.reporters.LogReporter" ;
        try {
            _reporter = (IReporter) Class.forName(reporterClassName).newInstance();
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

        _nimbusMonitorFreqSecs = Utils.getInt( conf.get(Config.NIMBUS_MONITOR_FREQ_SECS));
        blacklist = new TreeMap<>();
    }

    @Override
    public Set<String> getBlacklist(List<HashMap<String, Set<Integer>>> supervisorsWithFailures, Cluster cluster, Topologies topologies) {
        Map<String, Integer> countMap = new HashMap<String, Integer>();

        for (Map<String, Set<Integer>> item : supervisorsWithFailures) {
            Set<String> supervisors = item.keySet();
            for (String supervisor : supervisors) {
                int supervisorCount = 0;
                if (countMap.containsKey(supervisor)) {
                    supervisorCount = countMap.get(supervisor);
                }
                countMap.put(supervisor, supervisorCount + 1);
            }
        }
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            String supervisor = entry.getKey();
            int count = entry.getValue();
            if (count >= _toleranceCount) {
                if (!blacklist.containsKey(supervisor)) {// if not in blacklist then add it and set the resume time according to config
                    LOG.info("add supervisor {} to blacklist", supervisor);
                    LOG.info("supervisorsWithFailures : {}", supervisorsWithFailures);
                    _reporter.reportBlacklist(supervisor, supervisorsWithFailures);
                    blacklist.put(supervisor, _resumeTime / _nimbusMonitorFreqSecs);
                }
            }
        }
        releaseBlacklistWhenNeeded(cluster, topologies);
        return blacklist.keySet();
    }

    public void resumeFromBlacklist() {
        Set<String> readyToRemove = new HashSet<String>();
        for (Map.Entry<String, Integer> entry : blacklist.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue() - 1;
            if (value == 0) {
                readyToRemove.add(key);
            } else {
                blacklist.put(key, value);
            }
        }
        for (String key : readyToRemove) {
            blacklist.remove(key);
            LOG.info("supervisor {} reach the resume time ,removed from blacklist", key);
        }
    }

    public void releaseBlacklistWhenNeeded(Cluster cluster, Topologies topologies) {
        if (blacklist.size() > 0) {
            int totalNeedNumWorkers = 0;
            List<TopologyDetails> needSchedulingTopologies = cluster.needsSchedulingTopologies(topologies);
            for (TopologyDetails topologyDetails : needSchedulingTopologies) {
                int numWorkers = topologyDetails.getNumWorkers();
                int assignedNumWorkers = cluster.getAssignedNumWorkers(topologyDetails);
                int unAssignedNumWorkers = numWorkers - assignedNumWorkers;
                totalNeedNumWorkers += unAssignedNumWorkers;
            }
            Map<String, SupervisorDetails> availableSupervisors = cluster.getSupervisors();
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            int availableSlotsNotInBlacklistCount = 0;
            for (WorkerSlot slot : availableSlots) {
                if (!blacklist.containsKey(slot.getNodeId())) {
                    availableSlotsNotInBlacklistCount += 1;
                }
            }
            int shortage = totalNeedNumWorkers - availableSlotsNotInBlacklistCount;

            if (shortage > 0) {
                LOG.info("total needed num of workers :{}, available num of slots not in blacklist :{},num blacklist :{}, will release some blacklist."
                        , totalNeedNumWorkers, availableSlotsNotInBlacklistCount, blacklist.size());
                //release earliest blacklist
                Set<String> readyToRemove = new HashSet<String>();
                for (String supervisor : blacklist.keySet()) {//blacklist is treeMap sorted by value, value minimum meas earliest
                    if (availableSupervisors.containsKey(supervisor)) {
                        Set<Integer> ports = cluster.getAvailablePorts(availableSupervisors.get(supervisor));
                        readyToRemove.add(supervisor);
                        shortage -= ports.size();
                        if (shortage <= 0) {//released enough supervisor
                            break;
                        }
                    }
                }
                for (String key : readyToRemove) {
                    blacklist.remove(key);
                    LOG.info("release supervisor {} for shortage of worker slots.", key);
                }
            }
        }
    }
}
