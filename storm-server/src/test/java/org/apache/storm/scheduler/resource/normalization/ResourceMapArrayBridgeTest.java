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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ResourceMapArrayBridgeTest {

    private final String gpuResourceName = "gpu";
    private final String disksResourceName = "disks";
    
    private Map<String, Double> normalize(Map<String, ? extends Number> resources) {
        return NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resources);
    }
    
    @Test
    public void testCanTranslateBackAndForthBetweenMapAndArrayConsistently() {
        ResourceMapArrayBridge bridge = new ResourceMapArrayBridge();
        
        Map<String, Double> allResources = new HashMap<>();
        allResources.put(gpuResourceName, 2.0);
        allResources.put(disksResourceName, 64.0);
        Map<String, Double> normalizedResources = normalize(allResources);
        double[] resources = bridge.translateToResourceArray(normalizedResources);
        
        Map<String, Integer> resourceNamesToArrayIndex = bridge.getResourceNamesToArrayIndex();
        assertThat(resourceNamesToArrayIndex.size(), is(2));
        int gpuIndex = resourceNamesToArrayIndex.get(gpuResourceName);
        int disksIndex = resourceNamesToArrayIndex.get(disksResourceName);
        
        assertThat(resources.length, is(2));
        assertThat(resources[gpuIndex], is(2.0));
        assertThat(resources[disksIndex], is(64.0));
        
        Map<String, Double> roundTrippedResources = bridge.translateFromResourceArray(resources);
        assertThat(roundTrippedResources, is(normalizedResources));
        
        double[] roundTrippedResourceArray = bridge.translateToResourceArray(roundTrippedResources);
        assertThat(roundTrippedResourceArray, equalTo(resources));
    }
    
}
