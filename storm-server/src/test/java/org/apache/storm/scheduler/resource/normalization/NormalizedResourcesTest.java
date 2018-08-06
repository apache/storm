/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.normalization;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.storm.Constants;
import org.apache.storm.metric.StormMetricsRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NormalizedResourcesTest {
    
    @Rule
    public NormalizedResourcesRule normalizedResourcesRule = new NormalizedResourcesRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    private final String gpuResourceName = "gpu";
    
    private Map<String, Double> normalize(Map<String, ? extends Number> resources) {
        return NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resources);
    }
    
    @Test
    public void testAddCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        NormalizedResources addedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        resources.add(addedResources);
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(Constants.COMMON_CPU_RESOURCE_NAME), is(2.0));
        assertThat(resources.getTotalCpu(), is(2.0));
    }
    
    @Test
    public void testAddToExistingResource() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        NormalizedResources addedResources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        resources.add(addedResources);
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(gpuResourceName), is(2.0));
    }
    
    @Test
    public void testAddWhenOtherHasMoreResourcesThanThis() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.emptyMap()));
        NormalizedResources addedResources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        resources.add(addedResources);
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(gpuResourceName), is(1.0));
    }
    
    @Test
    public void testAddWhenOtherHasDifferentResourceThanThis() {
        String disks = "disks";
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(disks, 23)));
        NormalizedResources addedResources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        resources.add(addedResources);
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(disks), is(23.0));
        assertThat(normalizedMap.get(gpuResourceName), is(1.0));
    }
    
    @Test
    public void testRemoveZeroesWhenResourcesBecomeNegative() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        NormalizedResources removedResources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 2)));

        resources.remove(removedResources, new ResourceMetrics(new StormMetricsRegistry()));
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(gpuResourceName), is(0.0));
    }
    
    @Test
    public void testRemoveZeroesWhenCpuBecomesNegative() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        NormalizedResources removedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));

        resources.remove(removedResources, new ResourceMetrics(new StormMetricsRegistry()));
        assertThat(resources.getTotalCpu(), is(0.0));
    }
    
    @Test
    public void testRemoveFromCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources removedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        resources.remove(removedResources, new ResourceMetrics(new StormMetricsRegistry()));
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(Constants.COMMON_CPU_RESOURCE_NAME), is(1.0));
        assertThat(resources.getTotalCpu(), is(1.0));
    }
    
    @Test
    public void testRemoveFromExistingResources() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 15)));
        NormalizedResources removedResources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        resources.remove(removedResources, new ResourceMetrics(new StormMetricsRegistry()));
        
        Map<String, Double> normalizedMap = resources.toNormalizedMap();
        assertThat(normalizedMap.get(gpuResourceName), is(14.0));
    }

    @Test
    public void testCouldHoldWithTooFewCpus() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        NormalizedResources resourcesToCheck = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        
        boolean couldHold = resources.couldHoldIgnoringSharedMemory(resourcesToCheck, 100, 1);
        
        assertThat(couldHold, is(false));
    }
    
    @Test
    public void testCouldHoldWithTooFewResource() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        NormalizedResources resourcesToCheck = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 2)));
        
        boolean couldHold = resources.couldHoldIgnoringSharedMemory(resourcesToCheck, 100, 1);
        
        assertThat(couldHold, is(false));
    }
    
    @Test
    public void testCouldHoldWithTooLittleMemory() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        NormalizedResources resourcesToCheck = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        boolean couldHold = resources.couldHoldIgnoringSharedMemory(resourcesToCheck, 100, 200);
        
        assertThat(couldHold, is(false));
    }
    
    @Test
    public void testCouldHoldWithMissingResource() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.emptyMap()));
        NormalizedResources resourcesToCheck = new NormalizedResources(normalize(Collections.singletonMap(gpuResourceName, 1)));
        
        boolean couldHold = resources.couldHoldIgnoringSharedMemory(resourcesToCheck, 100, 1);
        
        assertThat(couldHold, is(false));
    }
    
    @Test
    public void testCouldHoldWithEnoughResources() {
        Map<String, Double> allResources = new HashMap<>();
        allResources.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResources.put(gpuResourceName, 2.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResources));
        NormalizedResources resourcesToCheck = new NormalizedResources(normalize(allResources));
        
        boolean couldHold = resources.couldHoldIgnoringSharedMemory(resourcesToCheck, 100, 100);
        
        assertThat(couldHold, is(true));
    }
    
    @Test
    public void testCalculateAvgUsageWithNoResourcesInTotal() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.emptyMap()));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.emptyMap()));
        
        double avg = resources.calculateAveragePercentageUsedBy(usedResources, 0, 0);
        
        assertThat(avg, is(100.0));
    }
    
    @Test
    public void testCalculateAvgWithOnlyCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        double avg = resources.calculateAveragePercentageUsedBy(usedResources, 0, 0);
        
        assertThat(avg, is(50.0));
    }
    
    @Test
    public void testCalculateAvgWithCpuAndMem() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        double avg = resources.calculateAveragePercentageUsedBy(usedResources, 4, 1);
        
        assertThat(avg, is((50.0 + 25.0)/2));
    }
    
    @Test
    public void testCalculateAvgWithCpuMemAndGenericResource() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 10.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        double avg = resources.calculateAveragePercentageUsedBy(usedResources, 4, 1);
        
        assertThat(avg, is((50.0 + 25.0 + 10.0)/3));
    }
    
    @Test
    public void testCalculateAvgWithUnusedResource() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 10.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        double avg = resources.calculateAveragePercentageUsedBy(usedResources, 4, 1);
        
        //The resource that is not used should count as if it is being used 0%
        assertThat(avg, is((50.0 + 25.0)/3));
    }
    
    @Test
    public void testCalculateAvgThrowsIfTotalIsMissingCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 5)));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateAveragePercentageUsedBy(usedResources, 0, 0);
    }
    
    @Test
    public void testCalculateAvgThrowsIfTotalIsMissingMemory() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateAveragePercentageUsedBy(usedResources, 100, 500);
    }
    
    @Test
    public void testCalculateAvgWithResourceMissingFromTotal() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateAveragePercentageUsedBy(usedResources, 4, 1);        
    }
    
    @Test
    public void testCalculateAvgWithTooLittleResourceInTotal() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 5.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateAveragePercentageUsedBy(usedResources, 4, 1);        
    }
    
    @Test
    public void testCalculateMinUsageWithNoResourcesInTotal() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.emptyMap()));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.emptyMap()));
        
        double min = resources.calculateMinPercentageUsedBy(usedResources, 0, 0);
        
        assertThat(min, is(100.0));
    }
    
    @Test
    public void testCalculateMinWithOnlyCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        double min = resources.calculateMinPercentageUsedBy(usedResources, 0, 0);
        
        assertThat(min, is(50.0));
    }
    
    @Test
    public void testCalculateMinWithCpuAndMem() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        double min = resources.calculateMinPercentageUsedBy(usedResources, 4, 1);
        
        assertThat(min, is(25.0));
    }
    
    @Test
    public void testCalculateMinWithCpuMemAndGenericResource() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 10.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        double min = resources.calculateMinPercentageUsedBy(usedResources, 4, 1);
        
        assertThat(min, is(10.0));
    }
    
    @Test
    public void testCalculateMinWithUnusedResource() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 10.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        double min = resources.calculateMinPercentageUsedBy(usedResources, 4, 1);
        
        //The resource that is not used should count as if it is being used 0%
        assertThat(min, is(0.0));
    }
    
    @Test
    public void testCalculateMinThrowsIfTotalIsMissingCpu() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 5)));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateMinPercentageUsedBy(usedResources, 0, 0);
    }
    
    @Test
    public void testCalculateMinThrowsIfTotalIsMissingMemory() {
        NormalizedResources resources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 2)));
        NormalizedResources usedResources = new NormalizedResources(normalize(Collections.singletonMap(Constants.COMMON_CPU_RESOURCE_NAME, 1)));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateMinPercentageUsedBy(usedResources, 100, 500);
    }
    
    @Test
    public void testCalculateMinWithResourceMissingFromTotal() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateMinPercentageUsedBy(usedResources, 4, 1);        
    }
    
    @Test
    public void testCalculateMinWithTooLittleResourceInTotal() {
        Map<String, Double> allResourcesMap = new HashMap<>();
        allResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 2.0);
        allResourcesMap.put(gpuResourceName, 1.0);
        NormalizedResources resources = new NormalizedResources(normalize(allResourcesMap));
        Map<String, Double> usedResourcesMap = new HashMap<>();
        usedResourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, 1.0);
        usedResourcesMap.put(gpuResourceName, 5.0);
        NormalizedResources usedResources = new NormalizedResources(normalize(usedResourcesMap));
        
        expectedException.expect(IllegalArgumentException.class);
        resources.calculateMinPercentageUsedBy(usedResources, 4, 1);        
    }
}
