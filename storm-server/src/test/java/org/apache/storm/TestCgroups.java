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

package org.apache.storm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CGroups
 */
public class TestCgroups {

    /**
     * Test whether cgroups are setup up correctly for use.  Also tests whether Cgroups produces the right command to
     * start a worker and cleans up correctly after the worker is shutdown
     */
    @Test
    public void testSetupAndTearDown() throws IOException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //We don't want to run the test is CGroups are not setup
        Assume
            .assumeTrue("Check if CGroups are setup", ((boolean) config.get(DaemonConfig.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE)) == true);

        assertTrue(stormCgroupHierarchyExists(config), "Check if STORM_CGROUP_HIERARCHY_DIR exists");
        assertTrue(stormCgroupSupervisorRootDirExists(config), "Check if STORM_SUPERVISOR_CGROUP_ROOTDIR exists");

        CgroupManager manager = new CgroupManager();
        manager.prepare(config);

        String workerId = UUID.randomUUID().toString();
        manager.reserveResourcesForWorker(workerId, 1024, 200, null);

        List<String> commandList = manager.getLaunchCommand(workerId, new ArrayList<>());
        StringBuilder command = new StringBuilder();
        for (String entry : commandList) {
            command.append(entry).append(" ");
        }
        String correctCommand1 = config.get(DaemonConfig.STORM_CGROUP_CGEXEC_CMD) + " -g memory,cpu:/"
                                 + config.get(DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " ";
        String correctCommand2 = config.get(DaemonConfig.STORM_CGROUP_CGEXEC_CMD) + " -g cpu,memory:/"
                                 + config.get(DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " ";
        assertTrue(command.toString().equals(correctCommand1) || command.toString().equals(correctCommand2),
            "Check if cgroup launch command is correct");

        String pathToWorkerCgroupDir = config.get(Config.STORM_CGROUP_HIERARCHY_DIR)
                                       + "/" + config.get(DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId;

        assertTrue(dirExists(pathToWorkerCgroupDir), "Check if cgroup directory exists for worker");

        /* validate cpu settings */

        String pathToCpuShares = pathToWorkerCgroupDir + "/cpu.shares";
        assertTrue(fileExists(pathToCpuShares), "Check if cpu.shares file exists");
        assertEquals("200", readFileAll(pathToCpuShares), "Check if the correct value is written into cpu.shares");

        /* validate memory settings */

        String pathTomemoryLimitInBytes = pathToWorkerCgroupDir + "/memory.limit_in_bytes";

        assertTrue(fileExists(pathTomemoryLimitInBytes), "Check if memory.limit_in_bytes file exists");
        assertEquals(String.valueOf(1024 * 1024 * 1024),
            readFileAll(pathTomemoryLimitInBytes),
            "Check if the correct value is written into memory.limit_in_bytes");

        String user = "dummy-user";
        int dummyPort = 0;
        manager.cleanup(user, workerId, dummyPort);

        assertFalse(dirExists(pathToWorkerCgroupDir), "Make sure cgroup was removed properly");
    }

    private boolean stormCgroupHierarchyExists(Map<String, Object> config) {
        String pathToStormCgroupHierarchy = (String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        return dirExists(pathToStormCgroupHierarchy);
    }

    private boolean stormCgroupSupervisorRootDirExists(Map<String, Object> config) {
        String pathTostormCgroupSupervisorRootDir = config.get(Config.STORM_CGROUP_HIERARCHY_DIR)
                                                    + "/" + config.get(DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR);

        return dirExists(pathTostormCgroupSupervisorRootDir);
    }

    private boolean dirExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && path.isDirectory();
    }

    private boolean fileExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && !path.isDirectory();
    }

    private String readFileAll(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Paths.get(filePath));
        return new String(data).trim();
    }
}
