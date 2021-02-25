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

package org.apache.storm.metric.cgroup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.CpuacctCore;
import org.apache.storm.container.cgroup.core.CpuacctCore.StatType;

/**
 * Report CPU used in the cgroup.
 */
@Deprecated
public class CGroupCpu extends CGroupMetricsBase<Map<String, Long>> {
    long previousSystem = 0;
    long previousUser = 0;
    private int userHz = -1;

    public CGroupCpu(Map<String, Object> conf) {
        super(conf, SubSystemType.cpuacct);
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public synchronized int getUserHZ() throws IOException {
        if (userHz < 0) {
            ProcessBuilder pb = new ProcessBuilder("getconf", "CLK_TCK");
            Process p = pb.start();
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = in.readLine().trim();
            userHz = Integer.valueOf(line);
        }
        return userHz;
    }

    @Override
    public Map<String, Long> getDataFrom(CgroupCore core) throws IOException {
        CpuacctCore cpu = (CpuacctCore) core;
        Map<StatType, Long> stat = cpu.getCpuStat();
        long systemHz = stat.get(StatType.system);
        long userHz = stat.get(StatType.user);
        long user = userHz - previousUser;
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        long sys = systemHz - previousSystem;
        previousUser = userHz;
        previousSystem = systemHz;
        long hz = getUserHZ();
        HashMap<String, Long> ret = new HashMap<>();
        ret.put("user-ms", user * 1000 / hz); //Convert to millis
        ret.put("sys-ms", sys * 1000 / hz);
        return ret;
    }
}
