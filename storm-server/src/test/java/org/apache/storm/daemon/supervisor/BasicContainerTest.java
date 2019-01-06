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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.daemon.supervisor.Container.ContainerType;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ConfigUtils;

public class BasicContainerTest {
    private static void setSystemProp(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static void checkpoint(Run r, String... newValues) throws Exception {
        if (newValues.length % 2 != 0) {
            throw new IllegalArgumentException("Parameters are of the form system property name, new value");
        }
        Map<String, String> orig = new HashMap<>();
        try {
            for (int index = 0; index < newValues.length; index += 2) {
                String key = newValues[index];
                String value = newValues[index + 1];
                orig.put(key, System.getProperty(key));
                setSystemProp(key, value);
            }
            r.run();
        } finally {
            for (Map.Entry<String, String> entry : orig.entrySet()) {
                setSystemProp(entry.getKey(), entry.getValue());
            }
        }
    }

    private static <T> void assertListEquals(List<T> a, List<T> b) {
        if (a == null) {
            assertNull(b);
        }
        if (b == null) {
            assertNull(a);
        }
        int commonLen = Math.min(a.size(), b.size());
        for (int i = 0; i < commonLen; i++) {
            assertEquals("at index " + i + "\n" + a + " !=\n" + b + "\n", a.get(i), b.get(i));
        }

        assertEquals("size of lists don't match \n" + a + " !=\n" + b, a.size(), b.size());
    }

    @Test
    public void testCreateNewWorkerId() throws Exception {
        final String topoId = "test_topology";
        final int supervisorPort = 6628;
        final int port = 8080;
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        Map<String, Object> superConf = new HashMap<>();
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);

        LocalState ls = mock(LocalState.class);

        MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
            "SUPERVISOR", supervisorPort, port, la, null, ls, null, new StormMetricsRegistry(),
            new HashMap<>(), ops, "profile");
        //null worker id means generate one...

        assertNotNull(mc._workerId);
        verify(ls).getApprovedWorkers();
        Map<String, Integer> expectedNewState = new HashMap<String, Integer>();
        expectedNewState.put(mc._workerId, port);
        verify(ls).setApprovedWorkers(expectedNewState);
    }

    @Test
    public void testRecovery() throws Exception {
        final String topoId = "test_topology";
        final String workerId = "myWorker";
        final int supervisorPort = 6628;
        final int port = 8080;
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        Map<String, Integer> workerState = new HashMap<String, Integer>();
        workerState.put(workerId, port);

        LocalState ls = mock(LocalState.class);
        when(ls.getApprovedWorkers()).thenReturn(workerState);

        Map<String, Object> superConf = new HashMap<>();
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);

        MockBasicContainer mc = new MockBasicContainer(ContainerType.RECOVER_FULL, superConf,
            "SUPERVISOR", supervisorPort, port, la, null, ls, null, new StormMetricsRegistry(),
            new HashMap<>(), ops, "profile");

        assertEquals(workerId, mc._workerId);
    }

    @Test
    public void testRecoveryMiss() throws Exception {
        final String topoId = "test_topology";
        final int supervisorPort = 6628;
        final int port = 8080;
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        Map<String, Integer> workerState = new HashMap<String, Integer>();
        workerState.put("somethingelse", port + 1);

        LocalState ls = mock(LocalState.class);
        when(ls.getApprovedWorkers()).thenReturn(workerState);

        try {
            new MockBasicContainer(ContainerType.RECOVER_FULL, new HashMap<String, Object>(),
                "SUPERVISOR", supervisorPort, port, la, null, ls, null, new StormMetricsRegistry(),
                new HashMap<>(), null, "profile");
            fail("Container recovered worker incorrectly");
        } catch (ContainerRecoveryException e) {
            //Expected
        }
    }

    @Test
    public void testCleanUp() throws Exception {
        final String topoId = "test_topology";
        final int supervisorPort = 6628;
        final int port = 8080;
        final String workerId = "worker-id";
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        Map<String, Object> superConf = new HashMap<>();
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        //Cleanup expects this dir to be there
        Files.createDirectories(ConfigUtils.workerPidsRoot(superConf, workerId));

        Map<String, Integer> workerState = new HashMap<String, Integer>();
        workerState.put(workerId, port);

        LocalState ls = mock(LocalState.class);
        when(ls.getApprovedWorkers()).thenReturn(new HashMap<>(workerState));

        MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
            "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(), new HashMap<>(), ops,
                                                       "profile");

        mc.cleanUp();

        assertNull(mc._workerId);
        verify(ls).getApprovedWorkers();
        Map<String, Integer> expectedNewState = new HashMap<String, Integer>();
        verify(ls).setApprovedWorkers(expectedNewState);
    }

    @Test
    public void testRunProfiling() throws Exception {
        final long pid = 100;
        final String topoId = "test_topology";
        final int supervisorPort = 6628;
        final int port = 8080;
        final String workerId = "worker-id";
        final Path stormLocal = ContainerTest.asAbsPath("tmp", "testing");
        final Path topoRoot = stormLocal.resolve(topoId).resolve(String.valueOf(port));
        final Path workerArtifactsPid = topoRoot.resolve("worker.pid");

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal.toString());
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal.toString());

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.slurpString(workerArtifactsPid)).thenReturn(String.valueOf(pid));

        LocalState ls = mock(LocalState.class);

        MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
            "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(),
            new HashMap<>(), ops, "profile");

        //HEAP DUMP
        ProfileRequest req = new ProfileRequest();
        req.set_action(ProfileAction.JMAP_DUMP);

        mc.runProfiling(req, false);

        assertEquals(1, mc.profileCmds.size());
        CommandRun cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "jmap", topoRoot.toString()), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);

        //JSTACK DUMP
        req.set_action(ProfileAction.JSTACK_DUMP);

        mc.runProfiling(req, false);

        assertEquals(1, mc.profileCmds.size());
        cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "jstack", topoRoot.toString()), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);

        //RESTART
        req.set_action(ProfileAction.JVM_RESTART);

        mc.runProfiling(req, false);

        assertEquals(1, mc.profileCmds.size());
        cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "kill"), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);

        //JPROFILE DUMP
        req.set_action(ProfileAction.JPROFILE_DUMP);

        mc.runProfiling(req, false);

        assertEquals(1, mc.profileCmds.size());
        cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "dump", topoRoot.toString()), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);

        //JPROFILE START
        req.set_action(ProfileAction.JPROFILE_STOP);

        mc.runProfiling(req, false);

        assertEquals(1, mc.profileCmds.size());
        cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "start"), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);

        //JPROFILE STOP
        req.set_action(ProfileAction.JPROFILE_STOP);

        mc.runProfiling(req, true);

        assertEquals(1, mc.profileCmds.size());
        cmd = mc.profileCmds.get(0);
        mc.profileCmds.clear();
        assertEquals(Arrays.asList("profile", String.valueOf(pid), "stop", topoRoot.toString()), cmd.cmd);
        assertEquals(topoRoot, cmd.pwd);
    }

    @Test
    public void testLaunch() throws Exception {
        final String topoId = "test_topology_current";
        final int supervisorPort = 6628;
        final int port = 8080;
        final Path stormHome = ContainerTest.asAbsPath("tmp", "storm-home");
        final Path stormLogDir = ContainerTest.asAbsPath(".", "target");
        final String workerId = "worker-id";
        final Path stormLocal = ContainerTest.asAbsPath("tmp", "storm-local");
        final Path distRoot = stormLocal.resolve("supervisor").resolve("stormdist").resolve(topoId);
        final Path stormcode = distRoot.resolve("stormcode.ser");
        final Path stormjar = distRoot.resolve("stormjar.jar");
        final Path log4jdir = stormHome.resolve("conf");
        final Path workerConf = log4jdir.resolve("worker.xml");
        final Path workerRoot = stormLocal.resolve("workers").resolve(workerId);
        final Path workerTmpDir = workerRoot.resolve("tmp");

        final StormTopology st = new StormTopology();
        st.set_spouts(new HashMap<>());
        st.set_bolts(new HashMap<>());
        st.set_state_spouts(new HashMap<>());
        byte[] serializedState = Utils.gzip(Utils.thriftSerialize(st));

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal.toString());
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal.toString());
        superConf.put(DaemonConfig.STORM_LOG4J2_CONF_DIR, log4jdir.toString());
        superConf.put(Config.WORKER_CHILDOPTS, " -Dtesting=true");

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.slurp(stormcode)).thenReturn(serializedState);

        LocalState ls = mock(LocalState.class);

        checkpoint(() -> {
                       MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
                "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(),
                new HashMap<>(), ops, "profile");

                       mc.launch();

                       assertEquals(1, mc.workerCmds.size());
                       CommandRun cmd = mc.workerCmds.get(0);
                       mc.workerCmds.clear();
                       assertListEquals(Arrays.asList(
                           "java",
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "org.apache.storm.LogWriter",
                           "java",
                           "-server",
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "-Dtesting=true",
                           "-Djava.library.path=JLP",
                           "-Dstorm.conf.file=",
                           "-Dstorm.options=",
                           "-Djava.io.tmpdir=" + workerTmpDir,
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "org.apache.storm.daemon.worker.Worker",
                           topoId,
                           "SUPERVISOR",
                           String.valueOf(supervisorPort),
                           String.valueOf(port),
                           workerId
                       ), cmd.cmd);
                       assertEquals(workerRoot, cmd.pwd);
                   },
                   ConfigUtils.STORM_HOME, stormHome.toString(),
                   "storm.log.dir", stormLogDir.toString());
    }

    @Test
    public void testLaunchStorm1version() throws Exception {
        final String topoId = "test_topology_storm_1.x";
        final int supervisorPort = 6628;
        final int port = 8080;
        final Path stormHome = ContainerTest.asAbsPath("tmp", "storm-home");
        final Path stormLogDir = ContainerTest.asAbsPath(".", "target");
        final String workerId = "worker-id";
        final Path stormLocal = ContainerTest.asAbsPath("tmp", "storm-local");
        final Path distRoot = stormLocal.resolve("supervisor").resolve("stormdist").resolve(topoId);
        final Path stormcode = distRoot.resolve("stormcode.ser");
        final Path stormjar = distRoot.resolve("stormjar.jar");
        final Path log4jdir = stormHome.resolve("conf");
        final Path workerConf = log4jdir.resolve("worker.xml");
        final Path workerRoot = stormLocal.resolve("workers").resolve(workerId);
        final Path workerTmpDir = workerRoot.resolve("tmp");

        final StormTopology st = new StormTopology();
        st.set_spouts(new HashMap<>());
        st.set_bolts(new HashMap<>());
        st.set_state_spouts(new HashMap<>());

        // minimum 1.x version of supporting STORM-2448 would be 1.0.4
        st.set_storm_version("1.0.4");
        byte[] serializedState = Utils.gzip(Utils.thriftSerialize(st));

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal.toString());
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal.toString());
        superConf.put(DaemonConfig.STORM_LOG4J2_CONF_DIR, log4jdir.toString());
        superConf.put(Config.WORKER_CHILDOPTS, " -Dtesting=true");

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.slurp(stormcode)).thenReturn(serializedState);

        LocalState ls = mock(LocalState.class);

        checkpoint(() -> {
                       MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
                "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(),
                new HashMap<>(), ops, "profile");

                       mc.launch();

                       assertEquals(1, mc.workerCmds.size());
                       CommandRun cmd = mc.workerCmds.get(0);
                       mc.workerCmds.clear();
                       assertListEquals(Arrays.asList(
                           "java",
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "org.apache.storm.LogWriter",
                           "java",
                           "-server",
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "-Dtesting=true",
                           "-Djava.library.path=JLP",
                           "-Dstorm.conf.file=",
                           "-Dstorm.options=",
                           "-Djava.io.tmpdir=" + workerTmpDir,
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "org.apache.storm.daemon.worker",
                           topoId,
                           "SUPERVISOR",
                           String.valueOf(port),
                           workerId
                       ), cmd.cmd);
                       assertEquals(workerRoot, cmd.pwd);
                   },
                   ConfigUtils.STORM_HOME, stormHome.toString(),
                   "storm.log.dir", stormLogDir.toString());
    }

    @Test
    public void testLaunchStorm0version() throws Exception {
        final String topoId = "test_topology_storm_0.x";
        final int supervisorPort = 6628;
        final int port = 8080;
        final Path stormHome = ContainerTest.asAbsPath("tmp", "storm-home");
        final Path stormLogDir = ContainerTest.asAbsPath(".", "target");
        final String workerId = "worker-id";
        final Path stormLocal = ContainerTest.asAbsPath("tmp", "storm-local");
        final Path distRoot = stormLocal.resolve("supervisor").resolve("stormdist").resolve(topoId);
        final Path stormcode = distRoot.resolve("stormcode.ser");
        final Path stormjar = distRoot.resolve("stormjar.jar");
        final Path log4jdir = stormHome.resolve("conf");
        final Path workerConf = log4jdir.resolve("worker.xml");
        final Path workerRoot = stormLocal.resolve("workers").resolve(workerId);
        final Path workerTmpDir = workerRoot.resolve("tmp");

        final StormTopology st = new StormTopology();
        st.set_spouts(new HashMap<>());
        st.set_bolts(new HashMap<>());
        st.set_state_spouts(new HashMap<>());

        // minimum 0.x version of supporting STORM-2448 would be 0.10.3
        st.set_storm_version("0.10.3");
        byte[] serializedState = Utils.gzip(Utils.thriftSerialize(st));

        final Map<String, Object> superConf = new HashMap<>();
        superConf.put(Config.STORM_LOCAL_DIR, stormLocal.toString());
        superConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, stormLocal.toString());
        superConf.put(DaemonConfig.STORM_LOG4J2_CONF_DIR, log4jdir.toString());
        superConf.put(Config.WORKER_CHILDOPTS, " -Dtesting=true");

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);
        when(ops.slurp(stormcode)).thenReturn(serializedState);

        LocalState ls = mock(LocalState.class);

        checkpoint(() -> {
                       MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
                "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(),
                new HashMap<>(), ops, "profile");

                       mc.launch();

                       assertEquals(1, mc.workerCmds.size());
                       CommandRun cmd = mc.workerCmds.get(0);
                       mc.workerCmds.clear();
                       assertListEquals(Arrays.asList(
                           "java",
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "backtype.storm.LogWriter",
                           "java",
                           "-server",
                           "-Dlogging.sensitivity=S3",
                           "-Dlogfile.name=worker.log",
                           "-Dstorm.home=" + stormHome,
                           "-Dworkers.artifacts=" + stormLocal,
                           "-Dstorm.id=" + topoId,
                           "-Dworker.id=" + workerId,
                           "-Dworker.port=" + port,
                           "-Dstorm.log.dir=" + stormLogDir,
                           "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
                           "-Dstorm.local.dir=" + stormLocal,
                           "-Dworker.memory_limit_mb=768",
                           "-Dlog4j.configurationFile=" + workerConf,
                           "-Dtesting=true",
                           "-Djava.library.path=JLP",
                           "-Dstorm.conf.file=",
                           "-Dstorm.options=",
                           "-Djava.io.tmpdir=" + workerTmpDir,
                           "-cp",
                           "FRAMEWORK_CP" + File.pathSeparator + stormjar.toAbsolutePath(),
                           "backtype.storm.daemon.worker",
                           topoId,
                           "SUPERVISOR",
                           String.valueOf(port),
                           workerId
                       ), cmd.cmd);
                       assertEquals(workerRoot, cmd.pwd);
                   },
                   ConfigUtils.STORM_HOME, stormHome.toString(),
                   "storm.log.dir", stormLogDir.toString());
    }

    @Test
    public void testSubstChildOpts() throws Exception {
        String workerId = "w-01";
        String topoId = "s-01";
        int supervisorPort = 6628;
        int port = 9999;
        int memOnheap = 512;

        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);

        Map<String, Object> superConf = new HashMap<>();

        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        when(ops.doRequiredTopoFilesExist(superConf, topoId)).thenReturn(true);

        LocalState ls = mock(LocalState.class);

        MockBasicContainer mc = new MockBasicContainer(ContainerType.LAUNCH, superConf,
            "SUPERVISOR", supervisorPort, port, la, null, ls, workerId, new StormMetricsRegistry(),
            new HashMap<>(), ops, "profile");

        assertListEquals(Arrays.asList(
            "-Xloggc:/tmp/storm/logs/gc.worker-9999-s-01-w-01-9999.log",
            "-Xms256m",
            "-Xmx512m"),
                         mc.substituteChildopts(
                             "-Xloggc:/tmp/storm/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log -Xms256m -Xmx%HEAP-MEM%m",
                             memOnheap));

        assertListEquals(Arrays.asList(
            "-Xloggc:/tmp/storm/logs/gc.worker-9999-s-01-w-01-9999.log",
            "-Xms256m",
            "-Xmx512m"),
                         mc.substituteChildopts(Arrays.asList(
                             "-Xloggc:/tmp/storm/logs/gc.worker-%ID%-%TOPOLOGY-ID%-%WORKER-ID%-%WORKER-PORT%.log", "-Xms256m",
                             "-Xmx%HEAP-MEM%m"), memOnheap));

        assertListEquals(Collections.emptyList(),
                         mc.substituteChildopts(null));
    }

    private static interface Run {
        public void run() throws Exception;
    }

    public static class CommandRun {
        final List<String> cmd;
        final Map<String, String> env;
        final Path pwd;

        public CommandRun(List<String> cmd, Map<String, String> env, Path pwd) {
            this.cmd = cmd;
            this.env = env;
            this.pwd = pwd;
        }
    }

    public static class MockBasicContainer extends BasicContainer {
        public final List<CommandRun> profileCmds = new ArrayList<>();
        public final List<CommandRun> workerCmds = new ArrayList<>();
        public MockBasicContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort,
                                  int port, LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager,
                                  LocalState localState, String workerId, StormMetricsRegistry metricsRegistry, 
                                  Map<String, Object> topoConf, AdvancedFSOps ops, String profileCmd) throws IOException {
            super(type, conf, supervisorId, supervisorPort, port, assignment, resourceIsolationManager, localState,
                  workerId, metricsRegistry,new ContainerMemoryTracker(metricsRegistry), topoConf, ops, profileCmd);
        }

        @Override
        protected Map<String, Object> readTopoConf() throws IOException {
            return new HashMap<>();
        }

        @Override
        public void createNewWorkerId() {
            super.createNewWorkerId();
        }

        @Override
        public List<String> substituteChildopts(Object value, int memOnheap) {
            return super.substituteChildopts(value, memOnheap);
        }

        @Override
        protected boolean runProfilingCommand(List<String> command, Map<String, String> env, String logPrefix,
                                              Path targetDir) throws IOException, InterruptedException {
            profileCmds.add(new CommandRun(command, env, targetDir));
            return true;
        }

        @Override
        protected void launchWorkerProcess(List<String> command, Map<String, String> env, String logPrefix,
                                           ExitCodeCallback processExitCallback, Path targetDir) throws IOException {
            workerCmds.add(new CommandRun(command, env, targetDir));
        }

        @Override
        protected String javaCmd(String cmd) {
            //avoid system dependent things
            return cmd;
        }

        @Override
        protected List<String> frameworkClasspath(SimpleVersion version) {
            //We are not really running anything so make this
            // simple to check for
            return Arrays.asList("FRAMEWORK_CP");
        }

        @Override
        protected String javaLibraryPath(Path stormRoot, Map<String, Object> conf) {
            return "JLP";
        }
    }
}
