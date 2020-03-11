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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;

/**
 * An interface to for implementing different scheduling strategies for the resource aware scheduling.
 */
public interface IStrategy {

    /**
     * Prepare the Strategy for scheduling.
     * @param config the cluster configuration
     */
    void prepare(Map<String, Object> config);

    /**
     * Initialize for the default implementation of schedule().
     *
     * @param sortNodesForEachExecutor Call sortNodes() before scheduling each executor.
     */
    void initForSchedule(boolean sortNodesForEachExecutor);

    /**
     * This method is invoked to calculate a scheduling for topology td.  Cluster will reject any changes that are
     * not for the given topology.  Any changes made to the cluster will be committed if the scheduling is successful.
     * <P></P>
     * NOTE: scheduling occurs as a runnable in an interruptible thread.  Scheduling should consider being interrupted if
     * long running.
     *
     * @param schedulingState the current state of the cluster
     * @param td the topology to schedule for
     * @return returns a SchedulingResult object containing SchedulingStatus object to indicate whether scheduling is
     *     successful.
     */
    SchedulingResult schedule(Cluster schedulingState, TopologyDetails td);

    /**
     * Scheduling uses sortAllNodes() which eventually uses a sortObjectResources()
     * method that can be overridden to provided different behavior.
     * <p>
     * sortObjectResources() method needs supporting classes ObjectResources,
     * AllResources and ExistingShceduleFunc that are also defined here.
     * </p>
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param exec                 executor for which the sorting is done
     * @param topologyDetails      topologyDetails for the above executor
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return a sorted list of ObjectResources
     */
    TreeSet<ObjectResources> sortObjectResources(
            AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
            ExistingScheduleFunc existingScheduleFunc);

    /**
     * a class to contain individual object resources as well as cumulative stats.
     */
    class AllResources {
        List<ObjectResources> objectResources = new LinkedList<>();
        final NormalizedResourceOffer availableResourcesOverall;
        final NormalizedResourceOffer totalResourcesOverall;
        String identifier;

        public AllResources(String identifier) {
            this.identifier = identifier;
            this.availableResourcesOverall = new NormalizedResourceOffer();
            this.totalResourcesOverall = new NormalizedResourceOffer();
        }

        public AllResources(AllResources other) {
            this(null,
                    new NormalizedResourceOffer(other.availableResourcesOverall),
                    new NormalizedResourceOffer(other.totalResourcesOverall),
                    other.identifier);
            List<ObjectResources> objectResourcesList = new ArrayList<>();
            for (ObjectResources objectResource : other.objectResources) {
                objectResourcesList.add(new ObjectResources(objectResource));
            }
            this.objectResources = objectResourcesList;
        }

        public AllResources(List<ObjectResources> objectResources, NormalizedResourceOffer availableResourcesOverall,
                            NormalizedResourceOffer totalResourcesOverall, String identifier) {
            this.objectResources = objectResources;
            this.availableResourcesOverall = availableResourcesOverall;
            this.totalResourcesOverall = totalResourcesOverall;
            this.identifier = identifier;
        }
    }

    /**
     * class to keep track of resources on a rack or node.
     */
    class ObjectResources {
        public final String id;
        public NormalizedResourceOffer availableResources;
        public NormalizedResourceOffer totalResources;
        public double effectiveResources = 0.0;

        public ObjectResources(String id) {
            this.id = id;
            this.availableResources = new NormalizedResourceOffer();
            this.totalResources = new NormalizedResourceOffer();
        }

        public ObjectResources(ObjectResources other) {
            this(other.id, other.availableResources, other.totalResources, other.effectiveResources);
        }

        public ObjectResources(String id, NormalizedResourceOffer availableResources, NormalizedResourceOffer totalResources,
                               double effectiveResources) {
            this.id = id;
            this.availableResources = availableResources;
            this.totalResources = totalResources;
            this.effectiveResources = effectiveResources;
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    /**
     * interface for calculating the number of existing executors scheduled on a object (rack or node).
     */
    interface ExistingScheduleFunc {
        int getNumExistingSchedule(String objectId);
    }

}
