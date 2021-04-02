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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;

/**
 * class to keep track of resources on a rack or node.
 */
public class ObjectResourcesItem {
    public final String id;
    public NormalizedResourceOffer availableResources;
    public NormalizedResourceOffer totalResources;

    /**
     * Amongst all {@link #availableResources}, this is the minimum ratio of resource to the total available in group.
     * Note that nodes are grouped into hosts. Hosts into racks. And racks are grouped under the cluster.
     *
     * <p>
     * An example of this calculation is in
     * {@link NodeSorter#sortObjectResourcesCommon(ObjectResourcesSummary, ExecutorDetails, NodeSorter.ExistingScheduleFunc)}
     * where value is calculated by {@link ObjectResourcesSummary#getAvailableResourcesOverall()}
     * using {@link NormalizedResourceOffer#calculateMinPercentageUsedBy(NormalizedResourceOffer)}.
     * </p>
     */
    public double minResourcePercent = 0.0;

    /**
     * Amongst all {@link #availableResources}, this is the average ratio of resource to the total available in group.
     * Note that nodes are grouped into hosts, hosts into racks, and racks are grouped under the cluster.
     *
     * <p>
     * An example of this calculation is in
     * {@link NodeSorter#sortObjectResourcesCommon(ObjectResourcesSummary, ExecutorDetails, NodeSorter.ExistingScheduleFunc)}
     * where value is calculated by {@link ObjectResourcesSummary#getAvailableResourcesOverall()}
     * using {@link NormalizedResourceOffer#calculateAveragePercentageUsedBy(NormalizedResourceOffer)}.
     * </p>
     */
    public double avgResourcePercent = 0.0;

    public ObjectResourcesItem(String id) {
        this.id = id;
        this.availableResources = new NormalizedResourceOffer();
        this.totalResources = new NormalizedResourceOffer();
    }

    public ObjectResourcesItem(ObjectResourcesItem other) {
        this(other.id, other.availableResources, other.totalResources, other.minResourcePercent, other.avgResourcePercent);
    }

    public ObjectResourcesItem(String id, NormalizedResourceOffer availableResources, NormalizedResourceOffer totalResources,
                               double minResourcePercent, double avgResourcePercent) {
        this.id = id;
        this.availableResources = availableResources;
        this.totalResources = totalResources;
        this.minResourcePercent = minResourcePercent;
        this.avgResourcePercent = avgResourcePercent;
    }

    public void add(ObjectResourcesItem other) {
        this.availableResources.add(other.availableResources);
        this.totalResources.add(other.totalResources);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
