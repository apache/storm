/*
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

/**
 * Report the percentage of the cpu guaranteed for the worker.
 * It gets the result from cpu.cfs_period_us and cpu.cfs_quota_us.
 * Use this when org.apache.storm.container.docker.DockerManager or org.apache.storm.container.oci.RuncLibContainerManager
 * is used as the storm.resource.isolation.plugin.
 */
public class CGroupCpuGuaranteeByCfsQuota extends CGroupMetricsBase implements WorkerMetricRegistrant {
    long guarantee = -1;

    public CGroupCpuGuaranteeByCfsQuota(Map<String, Object> conf) {
        super(conf, SubSystemType.cpu);
    }

    @Override
    public void registerMetrics(TopologyContext topologyContext) {
        if (enabled) {
            topologyContext.registerGauge("CGroupCpuGuaranteeByCfsQuota", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    if (guarantee < 0) {
                        CpuCore cpu = (CpuCore) core;
                        try {
                            long cpuCfsQuotaUs = cpu.getCpuCfsQuotaUs();
                            if (cpuCfsQuotaUs == -1) {
                                //cpu.cfs_quota_us = -1 indicates that the cgroup does not adhere to any CPU time restrictions.
                                guarantee = -1L;
                            } else {
                                long cpuCfsPeriodUs = cpu.getCpuCfsPeriodUs();
                                guarantee = cpuCfsQuotaUs * 100 / cpuCfsPeriodUs;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return guarantee;
                }
            });
        }
    }
}
