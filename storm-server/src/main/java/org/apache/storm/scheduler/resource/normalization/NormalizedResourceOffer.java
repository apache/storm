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

package org.apache.storm.scheduler.resource.normalization;

import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An offer of resources with normalized resource names.
 */
public class NormalizedResourceOffer implements NormalizedResourcesWithMemory {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResourceOffer.class);
    private final NormalizedResources normalizedResources;
    private double totalMemoryMb;

    /**
     * Create a new normalized resource offer.
     *
     * @param resources the resources to be normalized.
     */
    public NormalizedResourceOffer(Map<String, ? extends Number> resources) {
        Map<String, Double> normalizedResourceMap = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resources);
        totalMemoryMb = normalizedResourceMap.getOrDefault(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, 0.0);
        this.normalizedResources = new NormalizedResources(normalizedResourceMap);
    }

    /**
     * Create an offer with all resources set to 0.
     */
    public NormalizedResourceOffer() {
        normalizedResources = new NormalizedResources();
        totalMemoryMb = 0.0;
    }

    /**
     * Copy Constructor.
     * @param other what to copy.
     */
    public NormalizedResourceOffer(NormalizedResourceOffer other) {
        this.totalMemoryMb = other.totalMemoryMb;
        this.normalizedResources = new NormalizedResources(other.normalizedResources);
    }

    @Override
    public double getTotalMemoryMb() {
        return totalMemoryMb;
    }

    /**
     * Return these resources as a normalized map.
     * @return the normalized map.
     */
    public Map<String, Double> toNormalizedMap() {
        Map<String, Double> ret = normalizedResources.toNormalizedMap();
        ret.put(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, totalMemoryMb);
        return ret;
    }

    public void add(NormalizedResourcesWithMemory other) {
        normalizedResources.add(other.getNormalizedResources());
        totalMemoryMb += other.getTotalMemoryMb();
    }

    /**
     * Remove the resources in other from this.
     * @param other the resources to be removed.
     * @param resourceMetrics The resource related metrics
     * @return true if one or more resources in other were larger than available resources in this, else false.
     */
    public boolean remove(NormalizedResourcesWithMemory other, ResourceMetrics resourceMetrics) {
        boolean negativeResources = normalizedResources.remove(other.getNormalizedResources(), resourceMetrics);
        totalMemoryMb -= other.getTotalMemoryMb();
        if (totalMemoryMb < 0.0) {
            negativeResources = true;
            if (resourceMetrics != null) {
                resourceMetrics.getNegativeResourceEventsMeter().mark();
            }
            totalMemoryMb = 0.0;
        }
        return negativeResources;
    }

    public boolean remove(NormalizedResourcesWithMemory other) {
        return remove(other, null);
    }

    /**
     * Remove the resources in other from this.
     * @param other the resources to be removed.
     * @param resourceMetrics The resource related metrics
     * @return true if one or more resources in other were larger than available resources in this, else false.
     */
    public boolean remove(WorkerResources other, ResourceMetrics resourceMetrics) {
        boolean negativeResources = normalizedResources.remove(other);
        totalMemoryMb -= (other.get_mem_off_heap() + other.get_mem_on_heap());
        if (totalMemoryMb < 0.0) {
            negativeResources = true;
            resourceMetrics.getNegativeResourceEventsMeter().mark();
            totalMemoryMb = 0.0;
        }
        return negativeResources;
    }

    /**
     * Calculate the average percentage used.
     * @see NormalizedResources#calculateAveragePercentageUsedBy(org.apache.storm.scheduler.resource.normalization.NormalizedResources,
     *     double, double)
     */
    public double calculateAveragePercentageUsedBy(NormalizedResourceOffer used) {
        return normalizedResources.calculateAveragePercentageUsedBy(
            used.getNormalizedResources(), getTotalMemoryMb(), used.getTotalMemoryMb());
    }

    /**
     * Calculate the min percentage used of the resource.
     * @see NormalizedResources#calculateMinPercentageUsedBy(org.apache.storm.scheduler.resource.normalization.NormalizedResources, double,
     *     double)
     */
    public double calculateMinPercentageUsedBy(NormalizedResourceOffer used) {
        return normalizedResources.calculateMinPercentageUsedBy(used.getNormalizedResources(), getTotalMemoryMb(), used.getTotalMemoryMb());
    }

    /**
     * Check if resources might be able to fit.
     * @see NormalizedResources#couldHoldIgnoringSharedMemory(org.apache.storm.scheduler.resource.normalization.NormalizedResources, double,
     *     double)
     */
    public boolean couldHoldIgnoringSharedMemory(NormalizedResourcesWithMemory other) {
        return normalizedResources.couldHoldIgnoringSharedMemory(
            other.getNormalizedResources(), getTotalMemoryMb(), other.getTotalMemoryMb());
    }

    public boolean couldHoldIgnoringSharedMemoryAndCpu(NormalizedResourcesWithMemory other) {
        return normalizedResources.couldHoldIgnoringSharedMemoryAndCpu(
                other.getNormalizedResources(), getTotalMemoryMb(), other.getTotalMemoryMb());
    }

    public double getTotalCpu() {
        return normalizedResources.getTotalCpu();
    }

    @Override
    public NormalizedResources getNormalizedResources() {
        return normalizedResources;
    }

    @Override
    public String toString() {
        return "Normalized resources: " + toNormalizedMap();
    }

    /**
     * If a node or rack has a kind of resource not in a request, make that resource negative so when sorting that node or rack will
     * be less likely to be selected.
     * @param requestedResources the requested resources.
     */
    public void updateForRareResourceAffinity(NormalizedResourceRequest requestedResources) {
        normalizedResources.updateForRareResourceAffinity(requestedResources.getNormalizedResources());
    }

    @Override
    public void clear() {
        this.totalMemoryMb = 0.0;
        this.normalizedResources.clear();
    }

    @Override
    public boolean areAnyOverZero() {
        return totalMemoryMb > 0 || normalizedResources.areAnyOverZero();
    }

    /**
     * Is there any possibility that a resource request could ever fit on this.
     * @param minWorkerCpu the configured minimum worker CPU
     * @param requestedResources the requested resources
     * @return true if there is the possibility it might fit, no guarantee that it will, or false if there is no
     *     way it would ever fit.
     */
    public boolean couldFit(double minWorkerCpu, NormalizedResourceRequest requestedResources) {
        if (minWorkerCpu < 0.001) {
            return this.couldHoldIgnoringSharedMemory(requestedResources);
        } else {
            // Assume that there could be a worker already on the node that is under the minWorkerCpu budget.
            // It's possible we could combine with it.  Let's disregard minWorkerCpu from the request
            // and validate that CPU as a rough fit.
            double requestedCpu = Math.max(requestedResources.getTotalCpu() - minWorkerCpu, 0.0);
            if (requestedCpu > this.getTotalCpu()) {
                return false;
            }
            // now check memory only
            return this.couldHoldIgnoringSharedMemoryAndCpu(requestedResources);
        }
    }
}
