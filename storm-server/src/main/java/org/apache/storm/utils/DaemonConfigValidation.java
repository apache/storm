/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import static org.apache.storm.ServerConstants.NUMA_CORES;
import static org.apache.storm.ServerConstants.NUMA_GENERIC_RESOURCES_MAP;
import static org.apache.storm.ServerConstants.NUMA_MEMORY_IN_MB;
import static org.apache.storm.ServerConstants.NUMA_PORTS;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.validation.ConfigValidation;

public class DaemonConfigValidation {
    public static class NumaEntryValidator extends ConfigValidation.Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            Map<String, Object> numa = (Map<String, Object>) o;
            for (String key : new String[]{NUMA_CORES, NUMA_MEMORY_IN_MB, NUMA_PORTS}) {
                if (!numa.containsKey(key)) {
                    throw new IllegalArgumentException(
                            "The numa configuration key [" + key + "] is missing!"
                    );
                }
            }

            List<Integer> cores = (List<Integer>) numa.get(NUMA_CORES);
            Set<Integer> coreSet = new HashSet();
            coreSet.addAll(cores);
            if (coreSet.size() != cores.size()) {
                throw new IllegalArgumentException(
                        "Duplicate cores in NUMA config"
                );
            }
            try {
                Map<String, Double> numaGenericResources =
                        (Map<String, Double>) numa.getOrDefault(NUMA_GENERIC_RESOURCES_MAP, Collections.EMPTY_MAP);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid generic resources in NUMA config"
                );
            }
        }
    }
}
