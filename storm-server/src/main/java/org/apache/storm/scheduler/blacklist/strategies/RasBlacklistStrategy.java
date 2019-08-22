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

package org.apache.storm.scheduler.blacklist.strategies;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesWithMemory;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blacklisting strategy just like the default one, but specifically setup for use with the resource aware scheduler.
 */
public class RasBlacklistStrategy extends DefaultBlacklistStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RasBlacklistStrategy.class);

    @Override
    protected Set<String> releaseBlacklistWhenNeeded(Cluster cluster, final List<String> blacklistedNodeIds) {
        LOG.info("RAS We have {} nodes blacklisted...", blacklistedNodeIds.size());
        Set<String> readyToRemove = new HashSet<>();
        if (blacklistedNodeIds.size() > 0) {
            int availableSlots = cluster.getNonBlacklistedAvailableSlots(blacklistedNodeIds).size();
            int neededSlots = 0;
            NormalizedResourceOffer available = cluster.getNonBlacklistedClusterAvailableResources(blacklistedNodeIds);
            NormalizedResourceOffer needed = new NormalizedResourceOffer();

            for (TopologyDetails td : cluster.getTopologies()) {
                if (cluster.needsSchedulingRas(td)) {
                    int slots = 0;
                    try {
                        slots = ServerUtils.getEstimatedWorkerCountForRasTopo(td.getConf(), td.getTopology());
                    } catch (InvalidTopologyException e) {
                        LOG.warn("Could not guess the number of slots needed for {}", td.getName(), e);
                    }
                    int assignedSlots = cluster.getAssignedNumWorkers(td);
                    int tdSlotsNeeded = slots - assignedSlots;
                    neededSlots += tdSlotsNeeded;

                    NormalizedResourceRequest resources = td.getApproximateTotalResources();
                    needed.add(resources);

                    LOG.warn("{} needs to be scheduled with {} and {} slots", td.getName(), resources, tdSlotsNeeded);
                }
            }

            //Now we need to free up some resources...
            Map<String, SupervisorDetails> availableSupervisors = cluster.getSupervisors();
            NormalizedResourceOffer shortage = new NormalizedResourceOffer(needed);
            shortage.remove(available, cluster.getResourceMetrics());
            int shortageSlots = neededSlots - availableSlots;
            LOG.debug("Need {} and {} slots.", needed, neededSlots);
            LOG.debug("Available {} and {} slots.", available, availableSlots);
            LOG.debug("Shortage {} and {} slots.", shortage, shortageSlots);

            if (shortage.areAnyOverZero() || shortageSlots > 0) {
                LOG.info("Need {} and {} slots more. Releasing some blacklisted nodes to cover it.", shortage, shortageSlots);

                //release earliest blacklist - but release all supervisors on a given blacklisted host.
                Map<String, Set<String>> hostToSupervisorIds = createHostToSupervisorMap(blacklistedNodeIds, cluster);
                for (Set<String> supervisorIds : hostToSupervisorIds.values()) {
                    for (String supervisorId : supervisorIds) {
                        SupervisorDetails sd = availableSupervisors.get(supervisorId);
                        if (sd != null) {
                            NormalizedResourcesWithMemory sdAvailable = cluster.getAvailableResources(sd);
                            int sdAvailableSlots = cluster.getAvailablePorts(sd).size();
                            readyToRemove.add(supervisorId);
                            shortage.remove(sdAvailable, cluster.getResourceMetrics());
                            shortageSlots -= sdAvailableSlots;
                            LOG.info("Releasing {} with {} and {} slots leaving {} and {} slots to go", supervisorId,
                                    sdAvailable, sdAvailableSlots, shortage, shortageSlots);
                        }
                    }
                    // make sure we've handled all supervisors on the host before we break
                    if (!shortage.areAnyOverZero() && shortageSlots <= 0) {
                        // we have enough resources now...
                        break;
                    }
                }
            }
        }
        return readyToRemove;
    }
}
