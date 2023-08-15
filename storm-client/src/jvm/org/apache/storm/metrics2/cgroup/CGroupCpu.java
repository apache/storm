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

package org.apache.storm.metrics2.cgroup;

import com.codahale.metrics.Gauge;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CpuacctCore;
import org.apache.storm.metrics2.WorkerMetricRegistrant;
import org.apache.storm.task.TopologyContext;

/**
 * Report CPU used in the cgroup.
 */
public class CGroupCpu extends CGroupMetricsBase implements WorkerMetricRegistrant {
    private int userHz = -1;

    public CGroupCpu(Map<String, Object> conf) {
        super(conf, SubSystemType.cpuacct);
    }

    @Override
    public void registerMetrics(TopologyContext topologyContext) {
        if (enabled) {
            topologyContext.registerGauge("CGroupCpu.user-ms", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    CpuacctCore cpu = (CpuacctCore) core;
                    try {
                        Map<CpuacctCore.StatType, Long> stat = cpu.getCpuStat();
                        long userHz = stat.get(CpuacctCore.StatType.user);
                        long hz = getUserHz();
                        return userHz * 1000 / hz;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to get metric value", e);
                    }
                }
            });

            topologyContext.registerGauge("CGroupCpu.sys-ms", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    CpuacctCore cpu = (CpuacctCore) core;
                    try {
                        Map<CpuacctCore.StatType, Long> stat = cpu.getCpuStat();
                        long systemHz = stat.get(CpuacctCore.StatType.system);
                        long hz = getUserHz();
                        return systemHz * 1000 / hz;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to get metric value", e);
                    }
                }
            });
        }
    }

    private synchronized int getUserHz() throws IOException {
        if (userHz < 0) {
            ProcessBuilder pb = new ProcessBuilder("getconf", "CLK_TCK");
            Process p = pb.start();
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = in.readLine().trim();
            userHz = Integer.valueOf(line);
        }
        return userHz;
    }
}