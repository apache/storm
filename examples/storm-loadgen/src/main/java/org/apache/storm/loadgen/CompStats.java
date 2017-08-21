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

package org.apache.storm.loadgen;

import java.io.Serializable;
import java.util.Map;
import org.apache.storm.utils.ObjectReader;

/**
 * A set of measurements about a component (bolt/spout) so we can statistically reproduce it.
 */
public class CompStats implements Serializable {
    public final double cpuPercent; // Right now we don't have a good way to measure any kind of a distribution, this is all approximate
    public final double memoryMb; //again no good way to get a distribution...

    /**
     * Parse out a CompStats from a config map.
     * @param conf the map holding the CompStats values
     * @return the parsed CompStats
     */
    public static CompStats fromConf(Map<String, Object> conf) {
        double cpu = ObjectReader.getDouble(conf.get("cpuPercent"), 0.0);
        double memory = ObjectReader.getDouble(conf.get("memoryMb"), 0.0);
        return new CompStats(cpu, memory);
    }

    public void addToConf(Map<String, Object> ret) {
        ret.put("cpuPercent", cpuPercent);
        ret.put("memoryMb", memoryMb);
    }

    public CompStats(double cpuPercent, double memoryMb) {
        this.cpuPercent = cpuPercent;
        this.memoryMb = memoryMb;
    }
}
