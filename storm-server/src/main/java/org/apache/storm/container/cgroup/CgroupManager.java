/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.container.cgroup;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.SystemUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.DefaultResourceIsolationManager;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.CpusetCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.ExitCodeCallback;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements ResourceIsolationInterface that manages cgroups.
 */
public class CgroupManager extends DefaultResourceIsolationManager {

    private static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);
    private CgroupCenter center;
    private Hierarchy hierarchy;
    private CgroupCommon rootCgroup;
    private String rootDir;
    private Map<String, String> workerToNumaId;

    /**
     * initialize data structures.
     *
     * @param conf storm confs
     */
    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        super.prepare(conf);
        this.rootDir = DaemonConfig.getCgroupRootDir(this.conf);
        if (this.rootDir == null) {
            throw new RuntimeException("Check configuration file. The storm.supervisor.cgroup.rootdir is missing.");
        }

        File file = new File(DaemonConfig.getCgroupStormHierarchyDir(conf), rootDir);
        if (!file.exists()) {
            LOG.error("{} does not exist", file.getPath());
            throw new RuntimeException(
                "Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        this.center = CgroupCenter.getInstance();
        if (this.center == null) {
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        }
        this.prepareSubSystem(this.conf);
        workerToNumaId = new ConcurrentHashMap();
    }

    /**
     * Initialize subsystems.
     */
    private void prepareSubSystem(Map<String, Object> conf) throws IOException {
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : DaemonConfig.getCgroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        this.hierarchy = center.getHierarchyWithSubSystems(subSystemTypes);

        if (this.hierarchy == null) {
            Set<SubSystemType> types = new HashSet<>();
            types.add(SubSystemType.cpu);
            this.hierarchy = new Hierarchy(DaemonConfig.getCgroupStormHierarchyName(conf), types,
                                           DaemonConfig.getCgroupStormHierarchyDir(conf));
        }
        this.rootCgroup =
            new CgroupCommon(this.rootDir, this.hierarchy, this.hierarchy.getRootCgroups());

        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS
        // operations.
        CpuCore supervisorRootCpu = (CpuCore) this.rootCgroup.getCores().get(SubSystemType.cpu);
        setCpuUsageUpperLimit(supervisorRootCpu, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }

    /**
     * Use cfs_period & cfs_quota to control the upper limit use of cpu core e.g.
     * If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {
        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer totalMem, Integer cpuNum, String numaId) throws SecurityException {
        LOG.info("Creating cgroup for worker {} with resources {} MB {} % CPU", workerId, totalMem, cpuNum);
        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by
        // RAS (Resource Aware Scheduler)
        if (conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            cpuNum = ((Number) conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT)).intValue();
        }

        // The manually set STORM_WORKER_CGROUP_MEMORY_MB_LIMIT config on supervisor will overwrite
        // resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT) != null) {
            totalMem =
                ((Number) this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT)).intValue();
        }

        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        try {
            this.center.createCgroup(workerGroup);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating Cgroup! Exception: ", e);
        }

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuNum.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: ", e);
            }
        }

        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE)) {
            if (totalMem != null) {
                int cgroupMem =
                    (int)
                        (Math.ceil(
                            ObjectReader.getDouble(
                                this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB),
                                0.0)));
                long memLimit = Long.valueOf((totalMem.longValue() + cgroupMem) * 1024 * 1024);
                MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
                try {
                    memCore.setPhysicalUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: ", e);
                }
                // need to set memory.memsw.limit_in_bytes after setting memory.limit_in_bytes or error
                // might occur
                try {
                    memCore.setWithSwapUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.memsw.limit_in_bytes! Exception: ", e);
                }
            }
        }
        
        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_INHERIT_CPUSET_CONFIGS)) {
            if (workerGroup.getParent().getCores().containsKey(SubSystemType.cpuset)) {
                CpusetCore parentCpusetCore = (CpusetCore) workerGroup.getParent().getCores().get(SubSystemType.cpuset);
                CpusetCore cpusetCore = (CpusetCore) workerGroup.getCores().get(SubSystemType.cpuset);
                try {
                    cpusetCore.setCpus(parentCpusetCore.getCpus());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.cpus! Exception: ", e);
                }
                try {
                    cpusetCore.setMems(parentCpusetCore.getMems());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.mems! Exception: ", e);
                }
            }
        }

        if (numaId != null) {
            workerToNumaId.put(workerId, numaId);
        }
    }

    @Override
    public void cleanup(String user, String workerId, int port) throws IOException {

        CgroupCommon workerGroup = new CgroupCommon(workerId, hierarchy, this.rootCgroup);
        try {
            Set<Integer> tasks = workerGroup.getTasks();
            if (!tasks.isEmpty()) {
                throw new Exception("Cannot correctly shutdown worker CGroup " + workerId + "tasks " + tasks
                                    + " still running!");
            }
            this.center.deleteCgroup(workerGroup);
        } catch (Exception e) {
            LOG.error("Exception thrown when shutting worker {} Exception: {}", workerId, e);
        }
    }

    /**
     * Extracting out to mock it for tests.
     * @return true if on Linux.
     */
    protected static boolean isOnLinux() {
        return SystemUtils.IS_OS_LINUX;
    }

    private void prefixNumaPinning(List<String> command, String numaId) {
        if (isOnLinux()) {
            command.add(0, "numactl");
            command.add(1, "--cpunodebind=" + numaId);
            command.add(2, "--membind=" + numaId);
            return;
        } else {
            // TODO : Add support for pinning on Windows host
            throw new RuntimeException("numactl pinning currently not supported on non-Linux hosts");
        }
    }

    @Override
    public void launchWorkerProcess(String user, String topologyId, Map<String, Object> topoConf,
                                    int port, String workerId,
                                    List<String> command, Map<String, String> env, String logPrefix,
                                    ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        if (workerToNumaId.containsKey(workerId)) {
            prefixNumaPinning(command, workerToNumaId.get(workerId));
        }

        if (runAsUser) {
            String workerDir = targetDir.getAbsolutePath();
            List<String> args = Arrays.asList("worker", workerDir, ServerUtils.writeScript(workerDir, command, env));
            List<String> commandPrefix = getLaunchCommandPrefix(workerId);
            ClientSupervisorUtils.processLauncher(conf, user, commandPrefix, args, null,
                logPrefix, processExitCallback, targetDir);
        } else {
            command = getLaunchCommand(workerId, command);
            ClientSupervisorUtils.launchProcess(command, env, logPrefix, processExitCallback, targetDir);
        }
    }

    /**
     * To compose launch command based on workerId and existing command.
     * @param workerId the worker id
     * @param existingCommand the current command to run that may need to be modified
     * @return new commandline with necessary additions to launch worker
     */
    @VisibleForTesting
    public List<String> getLaunchCommand(String workerId, List<String> existingCommand) {
        List<String> newCommand = getLaunchCommandPrefix(workerId);

        if (workerToNumaId.containsKey(workerId)) {
            prefixNumaPinning(newCommand, workerToNumaId.get(workerId));
        }

        newCommand.addAll(existingCommand);
        return newCommand;
    }

    private List<String> getLaunchCommandPrefix(String workerId) {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);

        if (!this.rootCgroup.getChildren().contains(workerGroup)) {
            throw new RuntimeException(
                "cgroup " + workerGroup + " doesn't exist! Need to reserve resources for worker first!");
        }

        StringBuilder sb = new StringBuilder();

        sb.append(this.conf.get(DaemonConfig.STORM_CGROUP_CGEXEC_CMD)).append(" -g ");

        Iterator<SubSystemType> it = this.hierarchy.getSubSystems().iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
            if (it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }
        sb.append(workerGroup.getName());
        List<String> newCommand = new ArrayList<String>();
        newCommand.addAll(Arrays.asList(sb.toString().split(" ")));
        return newCommand;
    }

    private Set<Long> getRunningPids(String workerId) throws IOException {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        if (!this.rootCgroup.getChildren().contains(workerGroup)) {
            LOG.warn("cgroup {} doesn't exist!", workerGroup);
            return Collections.emptySet();
        }
        return workerGroup.getPids();
    }

    /**
     * Get all of the pids that are a part of this container.
     * @param workerId the worker id
     * @return all of the pids that are a part of this container
     */
    @Override
    protected Set<Long> getAllPids(String workerId) throws IOException {
        Set<Long> ret = super.getAllPids(workerId);
        Set<Long> morePids = getRunningPids(workerId);
        assert (morePids != null);
        ret.addAll(morePids);
        return ret;
    }

    @Override
    public long getMemoryUsage(String user, String workerId, int port) throws IOException {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
        return memCore.getPhysicalUsage();
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;
        try {
            MemoryCore memRoot = (MemoryCore) rootCgroup.getCores().get(SubSystemType.memory);
            if (memRoot != null) {
                //For cgroups no limit is max long.
                long limit = memRoot.getPhysicalUsageLimit();
                long used = memRoot.getMaxPhysicalUsage();
                rootCgroupLimitFree = (limit - used) / 1024 / 1024;
            }
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        return Long.min(rootCgroupLimitFree, ServerUtils.getMemInfoFreeMb());
    }

    @Override
    public boolean isResourceManaged() {
        return true;
    }
}
