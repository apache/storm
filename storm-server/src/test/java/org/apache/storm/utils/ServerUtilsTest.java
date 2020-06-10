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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.ZipFile;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.testing.TmpPath;
import org.junit.jupiter.api.Test;

public class ServerUtilsTest {

    @Test
    public void testExtractZipFileDisallowsPathTraversal() throws Exception {
        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path extractionDest = testRoot.resolve("dest");
            Files.createDirectories(extractionDest);

            /**
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

            /**
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
        Collection<Long> pids = getRunningProcessIds();

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
        assertEquals("User UID " + uid1 + " is not same as file " + p.toString() + " owner UID of " + uid2, uid1, uid2);
    }
}
