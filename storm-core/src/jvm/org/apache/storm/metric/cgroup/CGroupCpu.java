/**
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
package org.apache.storm.metric.cgroup;

import java.util.HashMap;
import java.util.Map;

/**
 * Report CPU used in the cgroup
 */
public class CGroupCpu extends CGroupMetricsBase<Map<String,Long>> {
    long previousSystem = 0;
    long previousUser = 0;
    
    public CGroupCpu(Map<String, Object> conf) {
        super(conf, "cpuacct.stat");
    }

    public int getUserHZ() {
        return 100; // On most systems (x86) this is fine.
        // If someone really does want full support
        // we need to run `getconf CLK_TCK` and cache the result.
    }

    @Override
    public Map<String, Long> parseFileContents(String contents) {
        try {
            long systemHz = 0;
            long userHz = 0;
            for (String line: contents.split("\n")) {
                if (!line.isEmpty()) {
                    String [] parts = line.toLowerCase().split("\\s+");
                    if (parts[0].contains("system")) {
                        systemHz = Long.parseLong(parts[1].trim());
                    } else if (parts[0].contains("user")) {
                        userHz = Long.parseLong(parts[1].trim());
                    }
                }   
            }
            long user = userHz - previousUser;
            long sys = systemHz - previousSystem;
            previousUser = userHz;
            previousSystem = systemHz;
            long hz = getUserHZ();
            HashMap<String, Long> ret = new HashMap<>();
            ret.put("user-ms", user * 1000/hz); //Convert to millis
            ret.put("sys-ms", sys * 1000/hz);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
