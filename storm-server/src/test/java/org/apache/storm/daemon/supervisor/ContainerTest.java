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

package org.apache.storm.daemon.supervisor;

import com.google.common.base.Joiner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.daemon.supervisor.Container.ContainerType;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.storm.metric.StormMetricsRegistry;

public class ContainerTest {
    private static final Joiner PATH_JOIN = Joiner.on(File.separator).skipNulls();
    private static final String DOUBLE_SEP = File.separator + File.separator;

    static String asAbsPath(String... parts) {
        return (File.separator + PATH_JOIN.join(parts)).replace(DOUBLE_SEP, File.separator);
    }

    static File asAbsFile(String... parts) {
        return new File(asAbsPath(parts));
    }

    static String asPath(String... parts) {
        return PATH_JOIN.join(parts);
    }

    public static File asFile(String... parts) {
        return new File(asPath(parts));
    }

    @Test
    public void testKill() throws Exception {
        final String topoId = "test_topology";
        final Map<String, Object> superConf = new HashMap<>();
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        MockContainer mc = new MockContainer(ContainerType.LAUNCH, superConf,
                                             "SUPERVISOR", 6628, 8080, la, null, "worker", new HashMap<>(), ops, new StormMetricsRegistry());
        mc.kill();
        assertEquals(Collections.EMPTY_LIST, mc.killedPids);
        assertEquals(Collections.EMPTY_LIST, mc.forceKilledPids);
        mc.forceKill();
        assertEquals(Collections.EMPTY_LIST, mc.killedPids);
        assertEquals(Collections.EMPTY_LIST, mc.forceKilledPids);

        long pid = 987654321;
        mc.allPids.add(pid);

        mc.kill();
        assertEquals(mc.allPids, new HashSet<>(mc.killedPids));
        assertEquals(Collections.EMPTY_LIST, mc.forceKilledPids);
        mc.killedPids.clear();

        mc.forceKill();
        assertEquals(Collections.EMPTY_LIST, mc.killedPids);
        assertEquals(mc.allPids, new HashSet<>(mc.forceKilledPids));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetup() throws Exception {
        final int port = 8080;
        final String topoId = "test_topology";
        final String workerId = "worker_id";
        final String user = "me";
        final String stormLocal = asAbsPath("tmp", "testing");
        final File workerArtifacts = asAbsFile(stormLocal, topoId, String.valueOf(port));
        final File logMetadataFile = new File(workerArtifacts, "worker.yaml");
        final File workerUserFile = asAbsFile(stormLocal, "workers-users", workerId);
        final File workerRoot = asAbsFile(stormLocal, "workers", workerId);
        final File distRoot = asAbsFile(stormLocal, "supervisor", "stormdist", topoId);

        final Map<String, Object> topoConf = new HashMap<>();
        final List<String> topoUsers = Arrays.asList("t-user-a", "t-user-b");
        final List<String> logUsers = Arrays.asList("l-user-a", "l-user-b");

        final List<String> topoGroups = Arrays.asList("t-group-a", "t-group-b");
        final List<String> logGroups = Arrays.asList("l-group-a", "l-group-b");

        topoConf.put(DaemonConfig.LOGS_GROUPS, logGroups);
        topoConf.put(Config.TOPOLOGY_GROUPS, topoGroups);
        topoConf.put(DaemonConfig.LOGS_USERS, logUsers);
        topoConf.put(Config.TOPOLOGY_USERS, topoUsers);

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal);
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal);

        final StringWriter yamlDump = new StringWriter();

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.fileExists(workerArtifacts)).thenReturn(true);
        when(ops.fileExists(workerRoot)).thenReturn(true);
        when(ops.getWriter(logMetadataFile)).thenReturn(yamlDump);

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        la.set_owner(user);
        MockContainer mc = new MockContainer(ContainerType.LAUNCH, superConf,
                                             "SUPERVISOR", 6628, 8080, la, null, workerId, topoConf, ops, new StormMetricsRegistry());

        mc.setup();

        //Initial Setup
        verify(ops).forceMkdir(new File(workerRoot, "pids"));
        verify(ops).forceMkdir(new File(workerRoot, "tmp"));
        verify(ops).forceMkdir(new File(workerRoot, "heartbeats"));
        verify(ops).fileExists(workerArtifacts);

        //Log file permissions
        verify(ops).getWriter(logMetadataFile);

        String yamlResult = yamlDump.toString();
        Yaml yaml = new Yaml();
        Map<String, Object> result = (Map<String, Object>) yaml.load(yamlResult);
        assertEquals(workerId, result.get("worker-id"));
        assertEquals(user, result.get(Config.TOPOLOGY_SUBMITTER_USER));
        HashSet<String> allowedUsers = new HashSet<>(topoUsers);
        allowedUsers.addAll(logUsers);
        assertEquals(allowedUsers, new HashSet<>(ObjectReader.getStrings(result.get(DaemonConfig.LOGS_USERS))));

        HashSet<String> allowedGroups = new HashSet<>(topoGroups);
        allowedGroups.addAll(logGroups);
        assertEquals(allowedGroups, new HashSet<>(ObjectReader.getStrings(result.get(DaemonConfig.LOGS_GROUPS))));

        //Save the current user to help with recovery
        verify(ops).dump(workerUserFile, user);

        //Create links to artifacts dir
        verify(ops).createSymlink(new File(workerRoot, "artifacts"), workerArtifacts);

        //Create links to blobs
        verify(ops, never()).createSymlink(new File(workerRoot, "resources"), new File(distRoot, "resources"));
    }

    @Test
    public void testCleanup() throws Exception {
        final int supervisorPort = 6628;
        final int port = 8080;
        final long pid = 100;
        final String topoId = "test_topology";
        final String workerId = "worker_id";
        final String user = "me";
        final String stormLocal = asAbsPath("tmp", "testing");
        final File workerArtifacts = asAbsFile(stormLocal, topoId, String.valueOf(port));
        final File logMetadataFile = new File(workerArtifacts, "worker.yaml");
        final File workerUserFile = asAbsFile(stormLocal, "workers-users", workerId);
        final File workerRoot = asAbsFile(stormLocal, "workers", workerId);
        final File workerPidsRoot = new File(workerRoot, "pids");

        final Map<String, Object> topoConf = new HashMap<>();

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal);
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal);

        final StringWriter yamlDump = new StringWriter();

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.fileExists(workerArtifacts)).thenReturn(true);
        when(ops.fileExists(workerRoot)).thenReturn(true);
        when(ops.getWriter(logMetadataFile)).thenReturn(yamlDump);

        ResourceIsolationInterface iso = mock(ResourceIsolationInterface.class);

        LocalAssignment la = new LocalAssignment();
        la.set_owner(user);
        la.set_topology_id(topoId);
        MockContainer mc = new MockContainer(ContainerType.LAUNCH, superConf,
                                             "SUPERVISOR", supervisorPort, port, la, iso, workerId, topoConf, ops, new StormMetricsRegistry());
        mc.allPids.add(pid);

        mc.cleanUp();
        verify(ops).deleteIfExists(eq(new File(workerPidsRoot, String.valueOf(pid))), eq(user), any(String.class));
        verify(iso).releaseResourcesForWorker(workerId);

        verify(ops).deleteIfExists(eq(new File(workerRoot, "pids")), eq(user), any(String.class));
        verify(ops).deleteIfExists(eq(new File(workerRoot, "tmp")), eq(user), any(String.class));
        verify(ops).deleteIfExists(eq(new File(workerRoot, "heartbeats")), eq(user), any(String.class));
        verify(ops).deleteIfExists(eq(workerRoot), eq(user), any(String.class));
        verify(ops).deleteIfExists(workerUserFile);
    }

    public static class MockContainer extends Container {

        public final List<Long> killedPids = new ArrayList<>();
        public final List<Long> forceKilledPids = new ArrayList<>();
        public final Set<Long> allPids = new HashSet<>();
        protected MockContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort,
                                int port, LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager,
                                String workerId, Map<String, Object> topoConf, AdvancedFSOps ops, StormMetricsRegistry metricsRegistry) throws IOException {
            super(type, conf, supervisorId, supervisorPort, port, assignment, resourceIsolationManager, workerId,
                  topoConf, ops, metricsRegistry, new ContainerMemoryTracker(new StormMetricsRegistry()));
        }

        @Override
        protected void kill(long pid) {
            killedPids.add(pid);
        }

        @Override
        protected void forceKill(long pid) {
            forceKilledPids.add(pid);
        }

        @Override
        protected Set<Long> getAllPids() throws IOException {
            return allPids;
        }

        @Override
        public void launch() throws IOException {
            fail("THIS IS NOT UNDER TEST");
        }

        @Override
        public void relaunch() throws IOException {
            fail("THIS IS NOT UNDER TEST");
        }

        @Override
        public boolean didMainProcessExit() {
            fail("THIS IS NOT UNDER TEST");
            return false;
        }

        @Override
        public boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException {
            fail("THIS IS NOT UNDER TEST");
            return false;
        }
    }

    private Collection<Long> getRunningProcessIds() throws IOException {
        // get list of few running processes
        Collection<Long> pids = new ArrayList<>();
        Process p = Runtime.getRuntime().exec(ServerUtils.IS_ON_WINDOWS ? "tasklist" : "ps -e");
        try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = input.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    pids.add(Long.parseLong(line.split("\\s")[0]));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        return pids;
    }

    @Test
    public void testIsProcessAlive() throws Exception {
        // specific selected process should not be alive for a randomly generated user
        String randomUser = RandomStringUtils.randomAlphanumeric(12);

        // get list of few running processes
        Collection<Long> pids = getRunningProcessIds();
        assertFalse(pids.isEmpty());
        for (long pid: pids) {
            boolean status = Container.isProcessAlive(pid, randomUser);
            assertFalse("Random user " + randomUser + " is not expected to own any process", status);
        }

        boolean status = false;
        String currentUser = System.getProperty("user.name");
        for (long pid: pids) {
            // at least one pid will be owned by the current user (doing the testing)
            if (Container.isProcessAlive(pid, currentUser)) {
                status = true;
                break;
            }
        }
        assertTrue("Expecting user " + currentUser + " to own at least one process", status);
    }

    @Test
    public void testIsAnyProcessAlive() throws Exception {
        // no process should be alive for a randomly generated user
        String randomUser = RandomStringUtils.randomAlphanumeric(12);
        Collection<Long> pids = getRunningProcessIds();

        assertFalse(pids.isEmpty());
        boolean status = Container.isAnyProcessAlive(pids, randomUser);
        assertFalse("Random user " + randomUser + " is not expected to own any process", status);

        // at least one pid will be owned by the current user (doing the testing)
        String currentUser = System.getProperty("user.name");
        status = Container.isAnyProcessAlive(pids, currentUser);
        assertTrue("Expecting user " + currentUser + " to own at least one process", status);

        if (!ServerUtils.IS_ON_WINDOWS) {
            // userid test is valid only on Posix platforms
            int inValidUserId = -1;
            status = Container.isAnyProcessAlive(pids, inValidUserId);
            assertFalse("Invalid userId " + randomUser + " is not expected to own any process", status);

            int currentUid = Container.getUserId(null);
            status = Container.isAnyProcessAlive(pids, currentUid);
            assertTrue("Expecting uid " + currentUid + " to own at least one process", status);
        }
    }

    @Test
    public void testGetUserId() throws Exception {
        if (ServerUtils.IS_ON_WINDOWS) {
            return; // trivially succeed on Windows, since this test is not for Windows platform
        }
        int uid1 = Container.getUserId(null);
        Path p = Files.createTempFile("testGetUser", ".txt");
        int uid2 = Container.getPathOwnerUid(p.toString());
        assertEquals("User UID " + uid1 + " is not same as file " + p.toString() + " owner UID of " + uid2, uid1, uid2);
    }
}
