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

package org.apache.storm.scheduler.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An offer of resources that has been normalized.
 */
public class NormalizedResourceOffer extends NormalizedResources {
    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResourceOffer.class);
    private double totalMemory;

    /**
     * Create a new normalized set of resources.  Note that memory is not covered here becasue it is not consistent in requests vs offers
     * because of how on heap vs off heap is used.
     *
     * @param resources the resources to be normalized.
     */
    public NormalizedResourceOffer(Map<String, ? extends Number> resources) {
        super(resources, null);
    }

    public NormalizedResourceOffer() {
        super(null, null);
    }

    public NormalizedResourceOffer(NormalizedResourceOffer other) {
        super(other);
        this.totalMemory = other.totalMemory;
    }

    @Override
    protected void initializeMemory(Map<String, Double> normalizedResources) {
        totalMemory = normalizedResources.getOrDefault(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, 0.0);
    }

    @Override
    public double getTotalMemoryMb() {
        return totalMemory;
    }

    @Override
    public String toString() {
        return super.toString() + " MEM: " + totalMemory;
    }

    @Override
    public Map<String,Double> toNormalizedMap() {
        Map<String, Double> ret = super.toNormalizedMap();
        ret.put(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, totalMemory);
        return ret;
    }

    @Override
    public void add(NormalizedResources other) {
        super.add(other);
        totalMemory += other.getTotalMemoryMb();
    }

    @Override
    public void remove(NormalizedResources other) {
        super.remove(other);
        totalMemory -= other.getTotalMemoryMb();
        assert totalMemory >= 0.0;
    }
}
