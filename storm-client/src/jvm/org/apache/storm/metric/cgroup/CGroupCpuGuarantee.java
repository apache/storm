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

package org.apache.storm.metric.cgroup;

import java.io.IOException;
import java.util.Map;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.CpuCore;

/**
 * Report the guaranteed number of ms this worker has requested.
 * It gets the result from cpu.shares.
 * Use this when org.apache.storm.container.cgroup.CgroupManager is used as the storm.resource.isolation.plugin.
 */
@Deprecated
public class CGroupCpuGuarantee extends CGroupMetricsBase<Long> {
    long previousTime = -1;

    public CGroupCpuGuarantee(Map<String, Object> conf) {
        super(conf, SubSystemType.cpu);
    }

    @Override
    public Long getDataFrom(CgroupCore core) throws IOException {
        CpuCore cpu = (CpuCore) core;
        Long msGuarantee = null;
        long now = System.currentTimeMillis();
        if (previousTime > 0) {
            long shares = cpu.getCpuShares();
            //By convention each share corresponds to 1% of a CPU core
            // or 100 = 1 core full time. So the guaranteed number of ms
            // (approximately) should be ...
            msGuarantee = (shares * (now - previousTime)) / 100;
        }
        previousTime = now;
        return msGuarantee;
    }
}
