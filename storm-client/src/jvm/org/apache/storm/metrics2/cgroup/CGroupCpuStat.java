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

import java.io.IOException;
import java.util.Map;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.metrics2.WorkerMetricRegistrant;
import org.apache.storm.task.TopologyContext;

public class CGroupCpuStat extends CGroupMetricsBase implements WorkerMetricRegistrant {

    public CGroupCpuStat(Map<String, Object> conf) {
        super(conf, SubSystemType.cpu);
    }

    @Override
    public void registerMetrics(TopologyContext topologyContext) {
        if (enabled) {
            topologyContext.registerGauge("CGroupCpuStat.nr.period-count", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        CpuCore.Stat stat = ((CpuCore) core).getCpuStat();
                        return stat.nrPeriods;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            topologyContext.registerGauge("CGroupCpuStat.nr.throttled-count", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        CpuCore.Stat stat = ((CpuCore) core).getCpuStat();
                        return stat.nrThrottled;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            topologyContext.registerGauge("CGroupCpuStat.nr.throttled-percentage", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        CpuCore.Stat stat = ((CpuCore) core).getCpuStat();
                        return (long) (stat.nrThrottled * 100.0 / stat.nrPeriods);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            topologyContext.registerGauge("CGroupCpuStat.throttled.time-ms", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        CpuCore.Stat stat = ((CpuCore) core).getCpuStat();
                        return Math.round(stat.throttledTime / 1000_000.0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }
}
