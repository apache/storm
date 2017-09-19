/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.eviction;

import java.util.Collection;
import java.util.Map;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEvictionStrategy implements IEvictionStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultEvictionStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;
    private RAS_Nodes nodes;

    @Override
    public boolean makeSpaceForTopo(TopologyDetails td, Cluster schedulingState, Map<String, User> userMap) {
        this.cluster = schedulingState;
        this.userMap = userMap;
        this.nodes = new RAS_Nodes(schedulingState);
        LOG.debug("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
        User submitter = this.userMap.get(td.getTopologySubmitter());
        if (submitter.getCpuResourceGuaranteed() == 0.0 || submitter.getMemoryResourceGuaranteed() == 0.0) {
            return false;
        }

        double cpuNeeded = td.getTotalRequestedCpu() / submitter.getCpuResourceGuaranteed();
        double memoryNeeded = (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap())
            / submitter.getMemoryResourceGuaranteed();

        User evictUser = this.findUserWithHighestAverageResourceUtilAboveGuarantee();
        //check if user has enough resource under his or her resource guarantee to schedule topology
        if ((1.0 - submitter.getCpuResourcePoolUtilization(schedulingState)) >= cpuNeeded
            && (1.0 - submitter.getMemoryResourcePoolUtilization(schedulingState)) >= memoryNeeded) {
            if (evictUser != null) {
                TopologyDetails topologyEvict = evictUser.getRunningTopologyWithLowestPriority(schedulingState);
                evictTopology(topologyEvict);
                return true;
            }
        } else {
            if (evictUser != null) {
                if ((evictUser.getResourcePoolAverageUtilization(schedulingState) - 1.0)
                    > (((cpuNeeded + memoryNeeded) / 2)
                        + (submitter.getResourcePoolAverageUtilization(schedulingState) - 1.0))) {
                    TopologyDetails topologyEvict = evictUser.getRunningTopologyWithLowestPriority(schedulingState);
                    evictTopology(topologyEvict);
                    return true;
                }
            }
        }
        //See if there is a lower priority topology that can be evicted from the current user
        //topologies should already be sorted in order of increasing priority.
        //Thus, topology at the front of the queue has the lowest priority
        for (TopologyDetails topo : submitter.getRunningTopologies(schedulingState)) {
            //check to if there is a topology with a lower priority we can evict
            if (topo.getTopologyPriority() > td.getTopologyPriority()) {
                LOG.debug("POTENTIALLY Evicting Topology {} from user {} (itself) since topology {} has a lower "
                        + "priority than topology {}", topo, submitter, topo, td);
                evictTopology(topo);
                return true;
            }
        }
        return false;
    }

    private void evictTopology(TopologyDetails topologyEvict) {
        Collection<WorkerSlot> workersToEvict = this.cluster.getUsedSlotsByTopologyId(topologyEvict.getId());

        LOG.info("Evicting Topology {} with workers: {} from user {}", topologyEvict.getName(), workersToEvict,
            topologyEvict.getTopologySubmitter());
        this.nodes.freeSlots(workersToEvict);
    }

    private User findUserWithHighestAverageResourceUtilAboveGuarantee() {
        double most = 0.0;
        User mostOverUser = null;
        for (User user : this.userMap.values()) {
            double over = user.getResourcePoolAverageUtilization(cluster) - 1.0;
            if ((over > most) && (!user.getRunningTopologies(cluster).isEmpty())) {
                most = over;
                mostOverUser = user;
            }
        }
        return mostOverUser;
    }
}
