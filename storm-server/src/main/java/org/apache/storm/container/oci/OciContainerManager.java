/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.storm.container.oci;

import static org.apache.storm.ServerConstants.NUMA_CORES;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OciContainerManager implements ResourceIsolationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(OciContainerManager.class);

    protected Map<String, Object> conf;
    protected List<String> readonlyBindmounts;
    protected List<String> readwriteBindmounts;
    protected String seccompJsonFile;
    protected String nscdPath;
    protected static final String TMP_DIR = File.separator + "tmp";
    protected String stormHome;
    protected String cgroupRootPath;
    protected String cgroupParent;

    protected String memoryCgroupRootPath;
    protected MemoryCore memoryCoreAtRoot;

    protected Map<String, Integer> workerToCpu = new ConcurrentHashMap<>();
    protected Map<String, Integer> workerToMemoryMb = new ConcurrentHashMap<>();
    protected Map<String, Object> validatedNumaMap = new ConcurrentHashMap();
    protected Map<String, List<String>> workerToCores = new ConcurrentHashMap<>();
    protected Map<String, String> workerToMemoryZone = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;

        readonlyBindmounts = ObjectReader.getStrings(conf.get(DaemonConfig.STORM_OCI_READONLY_BINDMOUNTS));

        readwriteBindmounts = ObjectReader.getStrings(conf.get(DaemonConfig.STORM_OCI_READWRITE_BINDMOUNTS));

        seccompJsonFile = (String) conf.get(DaemonConfig.STORM_OCI_SECCOMP_PROFILE);

        nscdPath = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_NSCD_DIR));

        stormHome = System.getProperty(ConfigUtils.STORM_HOME);

        cgroupRootPath = ObjectReader.getString(conf.get(Config.STORM_OCI_CGROUP_ROOT));

        cgroupParent = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_CGROUP_PARENT));

        if (!cgroupParent.startsWith(File.separator)) {
            cgroupParent = File.separator + cgroupParent;
            LOG.warn("{} is not an absolute path. Changing it to be absolute: {}", DaemonConfig.STORM_OCI_CGROUP_PARENT, cgroupParent);
        }

        memoryCgroupRootPath = cgroupRootPath + File.separator + "memory" + File.separator + cgroupParent;
        memoryCoreAtRoot = new MemoryCore(memoryCgroupRootPath);
        validatedNumaMap = SupervisorUtils.getNumaMap(conf);
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer workerMemoryMb, Integer workerCpu, String numaId) {
        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by
        // RAS (Resource Aware Scheduler)
        if (conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            workerCpu = ((Number) conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT)).intValue();
        }
        workerToCpu.put(workerId, workerCpu);

        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE)) {
            workerToMemoryMb.put(workerId, workerMemoryMb);
        }

        if (numaId != null) {
            Map<String, Object> numaIdEntry = (Map<String, Object>) validatedNumaMap.get(numaId);
            List<String> rawCores = ((List<Integer>) numaIdEntry.get(NUMA_CORES)).stream()
                .map(rawCore -> String.valueOf(rawCore)).collect(Collectors.toList());
            workerToCores.put(workerId, rawCores);
            workerToMemoryZone.put(workerId, numaId);
        }
    }

    @Override
    public void cleanup(String user, String workerId, int port) throws IOException {
        workerToCpu.remove(workerId);
        workerToMemoryMb.remove(workerId);
        workerToCores.remove(workerId);
        workerToMemoryZone.remove(workerId);
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;

        try {
            //For cgroups no limit is max long.
            long limit = memoryCoreAtRoot.getPhysicalUsageLimit();
            long used = memoryCoreAtRoot.getMaxPhysicalUsage();
            rootCgroupLimitFree = (limit - used) / 1024 / 1024;
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        return Long.min(rootCgroupLimitFree, ServerUtils.getMemInfoFreeMb());
    }

    /**
     * Get image name from topology Conf.
     * @param topoConf topology configuration
     * @return the image name
     */
    protected String getImageName(Map<String, Object> topoConf) {
        return (String) topoConf.get(Config.TOPOLOGY_OCI_IMAGE);
    }

    protected String commandFilePath(String dir, String commandTag) {
        return dir + File.separator + commandTag + ".sh";
    }

    protected String writeToCommandFile(String workerDir, String command, String commandTag) throws IOException {
        String scriptPath = commandFilePath(workerDir, commandTag);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(scriptPath))) {
            out.write(command);
        }
        LOG.debug("command : {}; location: {}", command, scriptPath);
        return scriptPath;
    }

    protected enum CmdType {
        LAUNCH_DOCKER_CONTAINER("launch-docker-container"),
        RUN_DOCKER_CMD("run-docker-cmd"),
        PROFILE_DOCKER_CONTAINER("profile-docker-container"),
        RUN_OCI_CONTAINER("run-oci-container"),
        REAP_OCI_CONTAINER("reap-oci-container"),
        PROFILE_OCI_CONTAINER("profile-oci-container");

        private final String name;

        CmdType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
