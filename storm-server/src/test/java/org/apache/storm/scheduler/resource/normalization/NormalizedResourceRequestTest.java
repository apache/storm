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
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.daemon.Acker;
import org.junit.Assert;
import org.junit.Test;

public class NormalizedResourceRequestTest {

    @Test
    public void testAckerCPUSetting() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT, 40);
        topoConf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 50);
        NormalizedResourceRequest request = new NormalizedResourceRequest(topoConf, Acker.ACKER_COMPONENT_ID);
        Map<String, Double> normalizedMap = request.toNormalizedMap();
        Double cpu = normalizedMap.get(Constants.COMMON_CPU_RESOURCE_NAME);
        Assert.assertNotNull(cpu);
        Assert.assertEquals(40, cpu, 0.001);
    }

    @Test
    public void testNonAckerCPUSetting() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT, 40);
        topoConf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 50);
        NormalizedResourceRequest request = new NormalizedResourceRequest(topoConf, "notAnAckerComponent");
        Map<String, Double> normalizedMap = request.toNormalizedMap();
        Double cpu = normalizedMap.get(Constants.COMMON_CPU_RESOURCE_NAME);
        Assert.assertNotNull(cpu);
        Assert.assertEquals(50, cpu, 0.001);
    }
}
