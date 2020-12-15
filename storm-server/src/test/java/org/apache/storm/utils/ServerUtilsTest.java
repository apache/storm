/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipFile;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.testing.TmpPath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerUtilsTest {

    public static final Logger LOG = LoggerFactory.getLogger(ServerUtilsTest.class);

    @Test
    public void testExtractZipFileDisallowsPathTraversal() throws Exception {
        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path extractionDest = testRoot.resolve("dest");
            Files.createDirectories(extractionDest);

            /*
             * Contains good.txt and ../evil.txt. Evil.txt will path outside the target dir, and should not be extracted.
             */
            try (ZipFile zip = new ZipFile(Paths.get("src/test/resources/evil-path-traversal.jar").toFile())) {
                ServerUtils.extractZipFile(zip, extractionDest.toFile(), null);
            }
            
            assertThat(Files.exists(extractionDest.resolve("good.txt")), is(true));
            assertThat(Files.exists(testRoot.resolve("evil.txt")), is(false));
        }
    }
    
    @Test
    public void testExtractZipFileDisallowsPathTraversalWhenUsingPrefix() throws Exception {
        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path destParent = testRoot.resolve("outer");
            Path extractionDest = destParent.resolve("resources");
            Files.createDirectories(extractionDest);

            /*
             * Contains resources/good.txt and resources/../evil.txt. Evil.txt should not be extracted as it would end
             * up outside the extraction dest.
             */
            try (ZipFile zip = new ZipFile(Paths.get("src/test/resources/evil-path-traversal-resources.jar").toFile())) {
                ServerUtils.extractZipFile(zip, extractionDest.toFile(), "resources");
            }
            
            assertThat(Files.exists(extractionDest.resolve("good.txt")), is(true));
            assertThat(Files.exists(extractionDest.resolve("evil.txt")), is(false));
            assertThat(Files.exists(destParent.resolve("evil.txt")), is(false));
        }
    }

    /**
     * Get running processes for all or specified user.
     *
     * @param user Null for all users, otherwise specify the user. e.g. "root"
     * @return List of ProcessIds for the user
     * @throws IOException
     */
    private Collection<Long> getRunningProcessIds(String user) throws IOException {
        // get list of few running processes
        Collection<Long> pids = new ArrayList<>();
        String cmd = ServerUtils.IS_ON_WINDOWS ? "tasklist" : (user == null) ? "ps -e" : "ps -U " + user ;
        Process p = Runtime.getRuntime().exec(cmd);
        try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = input.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("PID")) {
                    continue;
                }
                try {
                    String pidStr = line.split("\\s")[0];
                    if (pidStr.equalsIgnoreCase("pid")) {
                        continue; // header line
                    }
                    if (!StringUtils.isNumeric(pidStr)) {
                        LOG.debug("Ignoring line \"{}\" while looking for PIDs in output of \"{}\"", line, cmd);
                        continue;
                    }
                    pids.add(Long.parseLong(pidStr));
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
        Collection<Long> pids = getRunningProcessIds(null);
        assertFalse(pids.isEmpty());
        for (long pid: pids) {
            boolean status = ServerUtils.isProcessAlive(pid, randomUser);
            assertFalse("Random user " + randomUser + " is not expected to own any process", status);
        }

        boolean status = false;
        String currentUser = System.getProperty("user.name");
        for (long pid: pids) {
            // at least one pid will be owned by the current user (doing the testing)
            if (ServerUtils.isProcessAlive(pid, currentUser)) {
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
        Collection<Long> pids = getRunningProcessIds(null);

        assertFalse(pids.isEmpty());
        boolean status = ServerUtils.isAnyProcessAlive(pids, randomUser);
        assertFalse("Random user " + randomUser + " is not expected to own any process", status);

        // at least one pid will be owned by the current user (doing the testing)
        String currentUser = System.getProperty("user.name");
        status = ServerUtils.isAnyProcessAlive(pids, currentUser);
        assertTrue("Expecting user " + currentUser + " to own at least one process", status);

        if (!ServerUtils.IS_ON_WINDOWS) {
            // userid test is valid only on Posix platforms
            int inValidUserId = -1;
            status = ServerUtils.isAnyProcessAlive(pids, inValidUserId);
            assertFalse("Invalid userId " + randomUser + " is not expected to own any process", status);

            int currentUid = ServerUtils.getUserId(null);
            status = ServerUtils.isAnyProcessAlive(pids, currentUid);
            assertTrue("Expecting uid " + currentUid + " to own at least one process", status);
        }
    }

    @Test
    public void testGetUserId() throws Exception {
        if (ServerUtils.IS_ON_WINDOWS) {
            return; // trivially succeed on Windows, since this test is not for Windows platform
        }
        int uid1 = ServerUtils.getUserId(null);
        Path p = Files.createTempFile("testGetUser", ".txt");
        int uid2 = ServerUtils.getPathOwnerUid(p.toString());
        if (!p.toFile().delete()) {
            LOG.warn("Could not delete tempoary file {}", p);
        }
        assertEquals("User UID " + uid1 + " is not same as file " + p.toString() + " owner UID of " + uid2, uid1, uid2);
    }

    @Test
    public void testIsAnyProcessPosixProcessPidDirAlive() throws IOException {
        final String testName = "testIsAnyProcessPosixProcessPidDirAlive";
        List<String> errors = new ArrayList<>();
        int maxPidCnt = 5;
        if (ServerUtils.IS_ON_WINDOWS) {
            LOG.info("{}: test cannot be run on Windows. Marked as successful", testName);
            return;
        }
        final Path parentDir = Paths.get("/proc");
        if (!parentDir.toFile().exists()) {
            LOG.info("{}: test cannot be run on system without process directory {}, os.name={}",
                    testName, parentDir, System.getProperty("os.name"));
            // check if we can get process id on this Posix system - testing test code, useful on Mac
            String cmd = "/bin/sleep 10";
            if (getPidOfPosixProcess(Runtime.getRuntime().exec(cmd), errors) < 0) {
                fail(String.format("%s: Cannot obtain process id for executed command \"%s\"\n%s",
                        testName, cmd, String.join("\n\t", errors)));
            }
            return;
        }
        // Create processes and wait for their termination
        Set<Long> observables = new HashSet<>();

        for (int i = 0 ; i < maxPidCnt ; i++) {
            String cmd = "sleep 20000";
            Process process = Runtime.getRuntime().exec(cmd);
            long pid = getPidOfPosixProcess(process, errors);
            LOG.info("{}: ({}) ran process \"{}\" with pid={}", testName, i, cmd, pid);
            if (pid < 0) {
                String e = String.format("%s: (%d) Cannot obtain process id for executed command \"%s\"", testName, i, cmd);
                errors.add(e);
                LOG.error(e);
                continue;
            }
            observables.add(pid);
        }
        String userName = System.getProperty("user.name");
        // now kill processes one by one
        List<Long> pidList = new ArrayList<>(observables);
        final long processKillIntervalMs = 2000;
        for (int i = 0; i < pidList.size(); i++) {
            long pid = pidList.get(i);
            LOG.info("{}: ({}) Sleeping for {} milliseconds before kill", testName, i, processKillIntervalMs);
            if (sleepInterrupted(processKillIntervalMs)) {
                return;
            }
            Runtime.getRuntime().exec("kill -9 " + pid);
            LOG.info("{}: ({}) Sleeping for {} milliseconds after kill", testName, i, processKillIntervalMs);
            if (sleepInterrupted(processKillIntervalMs)) {
                return;
            }
            boolean pidDirsAvailable = ServerUtils.isAnyPosixProcessPidDirAlive(observables, userName);
            if (i < pidList.size() - 1) {
                if (pidDirsAvailable) {
                    LOG.info("{}: ({}) Found existing process directories before killing last process", testName, i);
                } else {
                    String e = String.format("%s: (%d) Found no existing process directories before killing last process", testName, i);
                    errors.add(e);
                    LOG.error(e);
                }
            } else {
                if (pidDirsAvailable) {
                    String e = String.format("%s: (%d) Found existing process directories after killing last process", testName, i);
                    errors.add(e);
                    LOG.error(e);
                } else {
                    LOG.info("{}: ({}) Found no existing process directories after killing last process", testName, i);
                }
            }
        }
        if (!errors.isEmpty()) {
            fail(String.format("There are %d failures in test:\n\t%s", errors.size(), String.join("\n\t", errors)));
        }
    }

    /**
     * Simulate the production scenario where the owner of the process directory is sometimes returned as the
     * UID instead of user. This scenario is simulated by calling
     * {@link ServerUtils#isAnyPosixProcessPidDirAlive(Collection, String, boolean)} with the last parameter
     * set to true as well as false.
     *
     * @throws Exception on I/O exception
     */
    @Test
    public void testIsAnyPosixProcessPidDirAliveMockingFileOwnerUid() throws Exception {
        File procDir = new File("/proc");
        if (!procDir.exists()) {
            LOG.info("Test testIsAnyPosixProcessPidDirAlive is designed to run on systems with /proc directory only, marking as success");
            return;
        }
        Collection<Long> allPids = getRunningProcessIds(null);
        Collection<Long> rootPids = getRunningProcessIds("root");
        assertFalse(allPids.isEmpty());
        assertFalse(rootPids.isEmpty());

        String currentUser = System.getProperty("user.name");

        for (boolean mockFileOwnerToUid: Arrays.asList(true, false)) {
            // at least one pid will be owned by the current user (doing the testing)
            boolean status = ServerUtils.isAnyPosixProcessPidDirAlive(allPids, currentUser, mockFileOwnerToUid);
            String err = String.format("(mockFileOwnerToUid=%s) Expecting user %s to own at least one process",
                    mockFileOwnerToUid, currentUser);
            assertTrue(err, status);
        }

        // simulate reassignment of all process id to a different user (root)
        for (boolean mockFileOwnerToUid: Arrays.asList(true, false)) {
            boolean status = ServerUtils.isAnyPosixProcessPidDirAlive(rootPids, currentUser, mockFileOwnerToUid);
            String err = String.format("(mockFileOwnerToUid=%s) Expecting user %s to own no process",
                    mockFileOwnerToUid, currentUser);
            assertFalse(err, status);
        }
    }

    /**
     * Make the best effort to obtain the Process ID from the Process object. Thus staying entirely with the JVM.
     *
     * @param p Process instance returned upon executing {@link Runtime#exec(String)}.
     * @param errors Populate errors when PID is a negative number.
     * @return positive PID upon success, otherwise negative.
     */
    private synchronized long getPidOfPosixProcess(Process p, List<String> errors) {
        Class<? extends Process> pClass = p.getClass();
        String pObjStr = ToStringBuilder.reflectionToString(p, ToStringStyle.SHORT_PREFIX_STYLE);
        String pclassName = pClass.getName();
        try {
            if (pclassName.equals("java.lang.UNIXProcess")) {
                Field f = pClass.getDeclaredField("pid");
                f.setAccessible(true);
                long pid = f.getLong(p);
                f.setAccessible(false);
                if (pid < 0) {
                    errors.add("\t \"pid\" attribute in Process class " + pclassName + " returned -1, process=" + pObjStr);
                }
                return pid;
            }
            for (Field f : pClass.getDeclaredFields()) {
                if (!f.getName().equalsIgnoreCase("pid")) {
                    continue;
                }
                LOG.info("ServerUtilsTest.getPidOfPosixProcess(): found attribute {}#{}", pclassName, f.getName());
                f.setAccessible(true);
                long pid = f.getLong(p);
                f.setAccessible(false);
                if (pid < 0) {
                    errors.add("\t \"pid\" attribute in Process class " + pclassName + " returned -1, process=" + pObjStr);
                }
                return pid;
            }
            // post JDK 9 there should be getPid() - future JDK-11 compatibility only for the sake of Travis test in community
            try {
                Method m = pClass.getDeclaredMethod("getPid");
                LOG.info("ServerUtilsTest.getPidOfPosixProcess(): found method {}#getPid()\n", pclassName);
                long pid = (Long)m.invoke(p);
                if (pid < 0) {
                    errors.add("\t \"getPid()\" method in Process class " + pclassName + " returned -1, process=" + pObjStr);
                }
                return pid;
            } catch (SecurityException e) {
                errors.add("\t getPid() method in Process class " + pclassName + " cannot be called: " + e.getMessage() + ", process=" + pObjStr);
                return -1;
            } catch (NoSuchMethodException e) {
                // ignore and try something else
            }
            errors.add("\t Process class " + pclassName + " missing field \"pid\" and missing method \"getPid()\", process=" + pObjStr);
            return -1;
        } catch (Exception e) {
            errors.add("\t Exception in Process class " + pclassName + ": " + e.getMessage() + ", process=" + pObjStr);
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Sleep for specified milliseconds and return true if sleep was interrupted.
     *
     * @param milliSeconds number of milliseconds to sleep
     * @return true if sleep was interrupted, false otherwise.
     */
    private boolean sleepInterrupted(long milliSeconds) {
        try {
            Thread.sleep(milliSeconds);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
    }
}
