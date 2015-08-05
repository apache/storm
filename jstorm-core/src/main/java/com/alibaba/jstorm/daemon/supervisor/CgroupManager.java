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
package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.container.CgroupCenter;
import com.alibaba.jstorm.container.Hierarchy;
import com.alibaba.jstorm.container.SubSystemType;
import com.alibaba.jstorm.utils.SystemOperation;
import com.alibaba.jstorm.container.cgroup.CgroupCommon;
import com.alibaba.jstorm.container.cgroup.core.CgroupCore;
import com.alibaba.jstorm.container.cgroup.core.CpuCore;
import com.alibaba.jstorm.utils.JStormUtils;

public class CgroupManager {

    public static final Logger LOG = LoggerFactory
            .getLogger(CgroupManager.class);

    public static final String JSTORM_HIERARCHY_NAME = "jstorm_cpu";

    public static final int ONE_CPU_SLOT = 1024;

    private CgroupCenter center;

    private Hierarchy h;

    private CgroupCommon rootCgroup;

    private static final String JSTORM_CPU_HIERARCHY_DIR = "/cgroup/cpu";
    private static String rootDir;

    public CgroupManager(Map conf) {
        LOG.info("running on cgroup mode");

        // Cgconfig service is used to create the corresponding cpu hierarchy
        // "/cgroup/cpu"
        rootDir = ConfigExtension.getCgroupRootDir(conf);
        if (rootDir == null)
            throw new RuntimeException(
                    "Check configuration file. The supervisor.cgroup.rootdir is missing.");

        File file = new File(JSTORM_CPU_HIERARCHY_DIR + "/" + rootDir);
        if (!file.exists()) {
            LOG.error(JSTORM_CPU_HIERARCHY_DIR + "/" + rootDir
                    + " is not existing.");
            throw new RuntimeException(
                    "Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        center = CgroupCenter.getInstance();
        if (center == null)
            throw new RuntimeException(
                    "Cgroup error, please check /proc/cgroups");
        this.prepareSubSystem();
    }

    private int validateCpuUpperLimitValue(int value) {
        /*
         * Valid value is -1 or 1~10 -1 means no control
         */
        if (value > 10)
            value = 10;
        else if (value < 1 && value != -1)
            value = 1;

        return value;
    }

    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit)
            throws IOException {
        /*
         * User cfs_period & cfs_quota to control the upper limit use of cpu
         * core e.g. If making a process to fully use two cpu cores, set
         * cfs_period_us to 100000 and set cfs_quota_us to 200000 The highest
         * value of "cpu core upper limit" is 10
         */
        cpuCoreUpperLimit = validateCpuUpperLimitValue(cpuCoreUpperLimit);

        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 100000);
        }
    }

    public String startNewWorker(Map conf, int cpuNum, String workerId)
            throws SecurityException, IOException {
        CgroupCommon workerGroup =
                new CgroupCommon(workerId, h, this.rootCgroup);
        this.center.create(workerGroup);
        CgroupCore cpu = workerGroup.getCores().get(SubSystemType.cpu);
        CpuCore cpuCore = (CpuCore) cpu;
        cpuCore.setCpuShares(cpuNum * ONE_CPU_SLOT);
        setCpuUsageUpperLimit(cpuCore,
                ConfigExtension.getWorkerCpuCoreUpperLimit(conf));

        StringBuilder sb = new StringBuilder();
        sb.append("cgexec -g cpu:").append(workerGroup.getName()).append(" ");
        return sb.toString();
    }

    public void shutDownWorker(String workerId, boolean isKilled) {
        CgroupCommon workerGroup =
                new CgroupCommon(workerId, h, this.rootCgroup);
        try {
            if (isKilled == false) {
                for (Integer pid : workerGroup.getTasks()) {
                    JStormUtils.kill(pid);
                }
                JStormUtils.sleepMs(1500);
            }
            center.delete(workerGroup);
        } catch (Exception e) {
            LOG.info("No task of " + workerId);
        }

    }

    public void close() throws IOException {
        this.center.delete(this.rootCgroup);
    }

    private void prepareSubSystem() {
        h = center.busy(SubSystemType.cpu);
        if (h == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            h =
                    new Hierarchy(JSTORM_HIERARCHY_NAME, types,
                            JSTORM_CPU_HIERARCHY_DIR);
        }
        rootCgroup = new CgroupCommon(rootDir, h, h.getRootCgroups());
    }
}
