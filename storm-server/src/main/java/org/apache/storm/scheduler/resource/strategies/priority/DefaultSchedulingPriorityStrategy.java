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

package org.apache.storm.scheduler.resource.strategies.priority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSchedulingPriorityStrategy implements ISchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulingPriorityStrategy.class);

    protected SimulatedUser getSimulatedUserFor(User u, ISchedulingState cluster) {
        return new SimulatedUser(u, cluster);
    }

    @Override
    public List<TopologyDetails> getOrderedTopologies(ISchedulingState cluster, Map<String, User> userMap) {
        double cpuAvail = cluster.getClusterTotalCpuResource();
        double memAvail = cluster.getClusterTotalMemoryResource();

        List<TopologyDetails> allUserTopologies = new ArrayList<>();
        List<SimulatedUser> users = new ArrayList<>();
        for (User u : userMap.values()) {
            users.add(getSimulatedUserFor(u, cluster));
        }
        while (!users.isEmpty()) {
            Collections.sort(users, new SimulatedUserComparator(cpuAvail, memAvail));
            SimulatedUser u = users.get(0);
            TopologyDetails td = u.getNextHighest();
            if (td == null) {
                users.remove(0);
            } else {
                double score = u.getScore(cpuAvail, memAvail);
                td = u.simScheduleNextHighest();
                LOG.info("SIM Scheduling {} with score of {}", td.getId(), score);
                cpuAvail -= td.getTotalRequestedCpu();
                memAvail -= (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap());
                allUserTopologies.add(td);
            }
        }
        return allUserTopologies;
    }

    protected static class SimulatedUser {
        public final double guaranteedCpu;
        public final double guaranteedMemory;
        protected final LinkedList<TopologyDetails> tds = new LinkedList<>();
        protected double assignedCpu = 0.0;
        protected double assignedMemory = 0.0;

        public SimulatedUser(User other, ISchedulingState cluster) {
            tds.addAll(cluster.getTopologies().getTopologiesOwnedBy(other.getId()));
            Collections.sort(tds, new TopologyByPriorityAndSubmissionTimeComparator());
            Double guaranteedCpu = other.getCpuResourceGuaranteed();
            if (guaranteedCpu == null) {
                guaranteedCpu = 0.0;
            }
            this.guaranteedCpu = guaranteedCpu;
            Double guaranteedMemory = other.getMemoryResourceGuaranteed();
            if (guaranteedMemory == null) {
                guaranteedMemory = 0.0;
            }
            this.guaranteedMemory = guaranteedMemory;
        }

        public TopologyDetails getNextHighest() {
            return tds.peekFirst();
        }

        public TopologyDetails simScheduleNextHighest() {
            TopologyDetails td = tds.pop();
            assignedCpu += td.getTotalRequestedCpu();
            assignedMemory += td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap();
            return td;
        }

        /**
         * Get a score for the simulated user.  This is used to sort the users, by their highest priority topology.
         * The only requirement is that if the user is over their guarantees, or there are no available resources the
         * returned score will be > 0.  If they are under their guarantee it must be negative.
         * @param availableCpu available CPU on the cluster.
         * @param availableMemory available memory on the cluster.
         * @param td the topology we are looking at.
         * @return the score.
         */
        protected double getScore(double availableCpu, double availableMemory, TopologyDetails td) {
            //(Requested + Assigned - Guaranteed)/Available
            if (td == null || availableCpu <= 0 || availableMemory <= 0) {
                return Double.MAX_VALUE;
            }
            double wouldBeCpu = assignedCpu + td.getTotalRequestedCpu();
            double wouldBeMem = assignedMemory + td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap();
            double cpuScore = (wouldBeCpu - guaranteedCpu) / availableCpu;
            double memScore = (wouldBeMem - guaranteedMemory) / availableMemory;
            return Math.max(cpuScore, memScore);
        }

        public double getScore(double availableCpu, double availableMemory) {
            TopologyDetails td = getNextHighest();
            return getScore(availableCpu, availableMemory, td);
        }

    }

    private static class SimulatedUserComparator implements Comparator<SimulatedUser> {

        private final double cpuAvail;
        private final double memAvail;

        private SimulatedUserComparator(double cpuAvail, double memAvail) {
            this.cpuAvail = cpuAvail;
            this.memAvail = memAvail;
        }

        @Override
        public int compare(SimulatedUser o1, SimulatedUser o2) {
            return Double.compare(o1.getScore(cpuAvail, memAvail), o2.getScore(cpuAvail, memAvail));
        }
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time.
     * First sort by Topology Priority, if there is a tie for topology priority, topology uptime is used to sort.
     */
    private static class TopologyByPriorityAndSubmissionTimeComparator implements Comparator<TopologyDetails> {

        @Override
        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            if (topo1.getTopologyPriority() > topo2.getTopologyPriority()) {
                return 1;
            } else if (topo1.getTopologyPriority() < topo2.getTopologyPriority()) {
                return -1;
            } else {
                if (topo1.getUpTime() > topo2.getUpTime()) {
                    return -1;
                } else if (topo1.getUpTime() < topo2.getUpTime()) {
                    return 1;
                } else {
                    return topo1.getId().compareTo(topo2.getId());
                }
            }
        }
    }
}