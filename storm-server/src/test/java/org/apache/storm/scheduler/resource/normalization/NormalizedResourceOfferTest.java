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

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.metric.StormMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;

public class NormalizedResourceOfferTest {
    @Test
    public void testNodeOverExtendedCpu() {
        NormalizedResourceOffer availableResources = createOffer(100.0, 0.0);
        NormalizedResourceOffer scheduledResources = createOffer(110.0, 0.0);
        availableResources.remove(scheduledResources, new ResourceMetrics(new StormMetricsRegistry()));
        Assert.assertEquals(0.0, availableResources.getTotalCpu(), 0.001);
    }

    @Test
    public void testNodeOverExtendedMemory() {
        NormalizedResourceOffer availableResources = createOffer(0.0, 5.0);
        NormalizedResourceOffer scheduledResources = createOffer(0.0, 10.0);
        availableResources.remove(scheduledResources, new ResourceMetrics(new StormMetricsRegistry()));
        Assert.assertEquals(0.0, availableResources.getTotalMemoryMb(), 0.001);
    }

    private NormalizedResourceOffer createOffer(Double cpu, Double memory) {
        Map<String, Double> resourceMap = new HashMap<>();
        resourceMap.put(Constants.COMMON_CPU_RESOURCE_NAME, cpu);
        resourceMap.put(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, memory);
        return new NormalizedResourceOffer(resourceMap);
    }
}
