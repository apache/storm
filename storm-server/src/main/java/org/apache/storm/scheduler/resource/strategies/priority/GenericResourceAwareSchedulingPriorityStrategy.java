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

package org.apache.storm.scheduler.resource.strategies.priority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.User;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericResourceAwareSchedulingPriorityStrategy extends DefaultSchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GenericResourceAwareSchedulingPriorityStrategy.class);

    @Override
    protected GrasSimulatedUser getSimulatedUserFor(User u, ISchedulingState cluster) {
        return new GrasSimulatedUser(u, cluster);
    }

    @Override
    public List<TopologyDetails> getOrderedTopologies(ISchedulingState cluster, Map<String, User> userMap) {
        double cpuAvail = cluster.getClusterTotalCpuResource();
        double memAvail = cluster.getClusterTotalMemoryResource();
        Map<String, Double> genericAvail = cluster.getClusterTotalGenericResources();

        List<TopologyDetails> allUserTopologies = new ArrayList<>();
        List<GrasSimulatedUser> users = new ArrayList<>();

        for (User u : userMap.values()) {
            users.add(getSimulatedUserFor(u, cluster));
        }

        while (!users.isEmpty()) {
            Collections.sort(users, new GrasSimulatedUserComparator(cpuAvail, memAvail, genericAvail));
            GrasSimulatedUser u = users.get(0);
            TopologyDetails td = u.getNextHighest();
            if (td == null) {
                users.remove(0);
            } else {
                double score = u.getScore(cpuAvail, memAvail, genericAvail);
                td = u.simScheduleNextHighest();
                LOG.info("GRAS SIM Scheduling {} with score of {}", td.getId(), score);
                cpuAvail -= td.getTotalRequestedCpu();
                memAvail -= (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap());
                for (Map.Entry<String, Double> entry : td.getTotalRequestedGenericResources().entrySet()) {
                    String resource = entry.getKey();
                    Double requestedAmount = entry.getValue();
                    if (!genericAvail.containsKey(resource)) {
                        LOG.warn("Resource: {} is not supported in this cluster. Ignoring this request.", resource);
                    } else {
                        genericAvail.put(resource, genericAvail.get(resource) - requestedAmount);
                    }
                }
                allUserTopologies.add(td);
            }
        }
        return allUserTopologies;
    }

    protected static class GrasSimulatedUser extends SimulatedUser {

        // Extend support for Generic Resources in addition to CPU and Memory
        public final Map<String, Double> guaranteedGenericResources;                // resource name : guaranteed amount
        private Map<String, Double> assignedGenericResources = new HashMap<>();     // resource name : assigned amount

        public GrasSimulatedUser(User other, ISchedulingState cluster) {
            super(other, cluster);

            Map<String, Double> guaranteedGenericResources = new HashMap<>();
            // generic resource types that are offered
            Set<String> availGenericResourceTypes = cluster.getClusterTotalGenericResources().keySet();
            for (String resourceType : availGenericResourceTypes) {
                Double guaranteedAmount = other.getGenericGuaranteed().getOrDefault(resourceType, 0.0);
                guaranteedGenericResources.put(resourceType, guaranteedAmount);
            }
            this.guaranteedGenericResources = guaranteedGenericResources;
        }

        @Override
        public TopologyDetails simScheduleNextHighest() {
            TopologyDetails td = super.simScheduleNextHighest();
            Map<String, Double> tdRequestedGenericResource = td.getTotalRequestedGenericResources();
            for (Map.Entry<String, Double> entry : tdRequestedGenericResource.entrySet()) {
                String resource = entry.getKey();
                Double requestedAmount = entry.getValue();
                assignedGenericResources.put(resource, assignedGenericResources.getOrDefault(resource, 0.0) + requestedAmount);
            }
            return td;
        }

        /**
         * Get a score for the simulated user.  This is used to sort the users, by their highest priority topology.
         * Only give user guarantees that will not exceed cluster capacity.
         * Score of each resource type is calculated as: (Requested + Assigned - Guaranteed)/clusterAvailable
         * The final score is a max over all resource types.
         * Topology score will fall into the following intervals if:
         *      User is under quota (guarantee):                    [(-guarantee)/available : 0]
         *      User is over quota:                                 (0, infinity)
         * Unfortunately, score below 0 does not guarantee that the topology will be scheduled due to resources fragmentation.
         * @param availableCpu available CPU on the cluster.
         * @param availableMemory available memory on the cluster.
         * @param availableGenericResources available generic resources (other that cpu and memory) in cluster
         * @param td the topology we are looking at.
         * @return the score.
         */
        protected double getScore(double availableCpu, double availableMemory,
                                  Map<String, Double> availableGenericResources, TopologyDetails td) {
            // calculate scores for cpu and memory first
            double ret = super.getScore(availableCpu, availableMemory, td);
            if (ret == Double.MAX_VALUE) {
                return ret;
            }
            Map<String, Double> tdTotalRequestedGeneric = td.getTotalRequestedGenericResources();
            if (tdTotalRequestedGeneric == null) {
                tdTotalRequestedGeneric = Collections.emptyMap();
            }
            for (Map.Entry<String, Double> entry : availableGenericResources.entrySet()) {
                String resource = entry.getKey();
                Double available = entry.getValue();
                if (available <= 0) {
                    return Double.MAX_VALUE;
                }
                Double wouldBeResource = assignedGenericResources.getOrDefault(resource, 0.0)
                    + tdTotalRequestedGeneric.getOrDefault(resource, 0.0);
                double thisScore = (wouldBeResource -  guaranteedGenericResources.getOrDefault(resource, 0.0)) / available;
                ret = Math.max(ret, thisScore);
            }

            return ret;
        }

        protected double getScore(double availableCpu, double availableMemory, Map<String, Double> availableGenericResources) {
            return getScore(availableCpu, availableMemory, availableGenericResources, getNextHighest());
        }
    }

    private static class GrasSimulatedUserComparator implements Comparator<GrasSimulatedUser> {
        private final double cpuAvail;
        private final double memAvail;
        private final Map<String, Double> genericAvail;

        private GrasSimulatedUserComparator(double cpuAvail, double memAvail, Map<String, Double> genericAvail) {
            this.cpuAvail = cpuAvail;
            this.memAvail = memAvail;
            this.genericAvail = genericAvail;
        }

        @Override
        public int compare(GrasSimulatedUser o1, GrasSimulatedUser o2) {
            return Double.compare(o1.getScore(cpuAvail, memAvail, genericAvail), o2.getScore(cpuAvail, memAvail, genericAvail));
        }
    }
}
