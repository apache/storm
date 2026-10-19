/*
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

package org.apache.storm.daemon.logviewer.handler;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import jakarta.ws.rs.core.Response;

import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

public class LogviewerLogPageHandlerTest {

    /**
     * list-log-files filter selects the correct log files to return.
     */
    @Test
    public void testListLogFiles() throws IOException {
        String rootPath = Files.createTempDirectory("workers-artifacts").toFile().getCanonicalPath();
        File file1 = new File(String.join(File.separator, rootPath, "topoA", "1111"), "worker.log");
        File file2 = new File(String.join(File.separator, rootPath, "topoA", "2222"), "worker.log");
        File file3 = new File(String.join(File.separator, rootPath, "topoB", "1111"), "worker.log");

        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        file3.createNewFile();

        String origin = "www.origin.server.net";
        Map<String, Object> stormConf = Utils.readStormConfig();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        LogviewerLogPageHandler handler = new LogviewerLogPageHandler(rootPath, rootPath,
                new WorkerLogs(stormConf, Paths.get(rootPath), metricsRegistry), new ResourceAuthorizer(stormConf), metricsRegistry);

        final Response expectedAll = LogviewerResponseBuilder.buildSuccessJsonResponse(
                List.of(String.join(File.separator, "topoA", "1111", "worker.log"),
                        String.join(File.separator, "topoA", "2222", "worker.log"),
                        String.join(File.separator, "topoB", "1111", "worker.log")),
                null,
                origin
        );

        final Response expectedFilterPort = LogviewerResponseBuilder.buildSuccessJsonResponse(
                List.of(String.join(File.separator, "topoA", "1111", "worker.log"),
                        String.join(File.separator, "topoB", "1111", "worker.log")),
                null,
                origin
        );

        final Response expectedFilterTopoId = LogviewerResponseBuilder.buildSuccessJsonResponse(
                List.of(String.join(File.separator, "topoB", "1111", "worker.log")),
                null,
                origin
        );

        final Response returnedAll = handler.listLogFiles("user", null, null, null, origin);
        final Response returnedFilterPort = handler.listLogFiles("user", 1111, null, null, origin);
        final Response returnedFilterTopoId = handler.listLogFiles("user", null, "topoB", null, origin);

        Utils.forceDelete(rootPath);

        assertEqualsJsonResponse(expectedAll, returnedAll, List.class);
        assertEqualsJsonResponse(expectedFilterPort, returnedFilterPort, List.class);
        assertEqualsJsonResponse(expectedFilterTopoId, returnedFilterTopoId, List.class);
    }

    /**
     * list-log-files only returns the log files the user is allowed to access.
     */
    @Test
    public void testListLogFilesFiltersFilesTheUserMayNotAccess() throws IOException {
        String rootPath = Files.createTempDirectory("workers-artifacts").toFile().getCanonicalPath();
        File file1 = new File(String.join(File.separator, rootPath, "topoA", "1111"), "worker.log");
        File file2 = new File(String.join(File.separator, rootPath, "topoA", "1111"), "worker.log.1");
        File file3 = new File(String.join(File.separator, rootPath, "topoB", "1111"), "worker.log");

        file1.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        file3.createNewFile();

        String origin = "www.origin.server.net";
        String topoAPortDir = String.join(File.separator, "topoA", "1111");
        Map<String, Object> stormConf = Utils.readStormConfig();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
        when(resourceAuthorizer.isUserAllowedToAccessFile(anyString(), startsWith(topoAPortDir))).thenReturn(true);
        LogviewerLogPageHandler handler = new LogviewerLogPageHandler(rootPath, rootPath,
                new WorkerLogs(stormConf, Paths.get(rootPath), metricsRegistry), resourceAuthorizer, metricsRegistry);

        final Response returned = handler.listLogFiles("user", null, null, null, origin);

        List<?> files = new ObjectMapper().readValue((String) returned.getEntity(), List.class);

        Utils.forceDelete(rootPath);

        assertEquals(List.of(String.join(File.separator, topoAPortDir, "worker.log"),
                String.join(File.separator, topoAPortDir, "worker.log.1")), files);
        //The authorization only depends on the port directory, so it is checked once per port directory, not once per file.
        verify(resourceAuthorizer, times(2)).isUserAllowedToAccessFile(anyString(), anyString());
    }

    private <T> void assertEqualsJsonResponse(Response expected, Response actual, Class<T> entityClass) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        T entityFromExpected = objectMapper.readValue((String) expected.getEntity(), entityClass);
        T entityFromActual = objectMapper.readValue((String) actual.getEntity(), entityClass);
        assertEquals(entityFromExpected, entityFromActual);

        assertEquals(expected.getStatus(), actual.getStatus());
        assertTrue(expected.getHeaders().equalsIgnoreValueOrder(actual.getHeaders()));
    }

    @Test
    public void testListLogFilesOutsideLogRoot() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            String origin = "www.origin.server.net";
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath());

            //The response should be empty, since you should not be able to list files outside the worker log root.
            final Response expected = LogviewerResponseBuilder.buildSuccessJsonResponse(
                List.of(),
                null,
                origin
            );

            final Response returned = handler.listLogFiles("user", null, "../", null, origin);

            assertEqualsJsonResponse(expected, returned, List.class);
        }
    }

    @Test
    public void testLogPageOutsideLogRoot() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath());

            final Response returned = handler.logPage("../nimbus.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            //Should not show files outside worker log root.
            assertThat(returned.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDaemonLogPageOutsideLogRoot() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath());

            final Response returned = handler.daemonLogPage("../evil.sh", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            //Should not show files outside daemon log root.
            assertThat(returned.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDaemonLogPagePathIntoWorkerLogs() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath());

            final Response returned = handler.daemonLogPage("workers-artifacts/topoA/worker.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            //Should not show files outside log root.
            assertThat(returned.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDaemonLogPageUnauthorizedUser() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
            when(resourceAuthorizer.isUserAllowedToAccessDaemonFile(anyString())).thenReturn(false);
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath(), resourceAuthorizer);
            //Give the daemon log some content, so that an unauthorized request is the only reason not to render the page.
            Files.writeString(rootPath.getFile().toPath().resolve("logs").resolve("nimbus.log"), "nimbus log content");

            final Response returned = handler.daemonLogPage("nimbus.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.FORBIDDEN.getStatusCode()));
        }
    }

    @Test
    public void testDaemonLogPageAuthorizedUser() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
            when(resourceAuthorizer.isUserAllowedToAccessDaemonFile(anyString())).thenReturn(true);
            LogviewerLogPageHandler handler = createHandlerForTraversalTests(rootPath.getFile().toPath(), resourceAuthorizer);
            Files.writeString(rootPath.getFile().toPath().resolve("logs").resolve("nimbus.log"), "nimbus log content");

            final Response returned = handler.daemonLogPage("nimbus.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) returned.getEntity(), containsString("nimbus log content"));
            verify(resourceAuthorizer).isUserAllowedToAccessDaemonFile("user");
        }
    }

    @Test
    public void testLogPageDoesNotFollowSymbolicLinkToAnotherTopology() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createWorkerLogTree(rootPath.getFile().toPath().resolve("logs"));
            Files.createSymbolicLink(workerLogRoot.resolve("topoA").resolve("1111").resolve("worker.log.1"),
                workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.log"));

            WorkerLogs workerLogs = mock(WorkerLogs.class);
            LogviewerLogPageHandler handler = createHandlerAllowingOnlyTopoA(rootPath.getFile().toPath().resolve("logs"), workerLogs);

            final Response returned = handler.logPage("topoA/1111/worker.log.1", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat("rendered page: " + returned.getEntity(), returned.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            verify(workerLogs, never()).setLogFilePermission(anyString());
        }
    }

    @Test
    public void testLogPageDoesNotListSymbolicLinks() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createWorkerLogTree(rootPath.getFile().toPath().resolve("logs"));
            Files.createSymbolicLink(workerLogRoot.resolve("topoA").resolve("1111").resolve("worker.log.1"),
                workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.log"));

            LogviewerLogPageHandler handler = createHandlerAllowingOnlyTopoA(rootPath.getFile().toPath().resolve("logs"),
                mock(WorkerLogs.class));

            final Response returned = handler.logPage("topoA/1111/worker.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.OK.getStatusCode()));
            String page = (String) returned.getEntity();
            assertThat(page, containsString("topoA content"));
            assertThat(page, not(containsString("worker.log.1")));
        }
    }

    @Test
    public void testLogPageUnderSymbolicLinkRoot() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path realDaemonLogRoot = rootPath.getFile().toPath().resolve("real-logs");
            createWorkerLogTree(realDaemonLogRoot);
            Path linkedDaemonLogRoot = rootPath.getFile().toPath().resolve("logs");
            Files.createSymbolicLink(linkedDaemonLogRoot, realDaemonLogRoot);

            WorkerLogs workerLogs = mock(WorkerLogs.class);
            LogviewerLogPageHandler handler = createHandlerAllowingOnlyTopoA(linkedDaemonLogRoot, workerLogs);

            final Response returned = handler.logPage("topoA/1111/worker.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) returned.getEntity(), containsString("topoA content"));
            verify(workerLogs).setLogFilePermission("topoA/1111/worker.log");
        }
    }

    @Test
    public void testLogPageRejectsFileBelowSubdirectoryOfPortDir() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createWorkerLogTree(rootPath.getFile().toPath().resolve("logs"));
            Path nested = workerLogRoot.resolve("topoA").resolve("1111").resolve("sub").resolve("worker.log");
            Files.createDirectories(nested.getParent());
            Files.writeString(nested, "nested content");

            WorkerLogs workerLogs = mock(WorkerLogs.class);
            LogviewerLogPageHandler handler = createHandlerAllowingOnlyTopoA(rootPath.getFile().toPath().resolve("logs"), workerLogs);

            final Response returned = handler.logPage("topoA/1111/sub/worker.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            verify(workerLogs, never()).setLogFilePermission(anyString());
        }
    }

    @Test
    public void testLogPageServesRotatedAndCompressedWorkerLogs() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createWorkerLogTree(rootPath.getFile().toPath().resolve("logs"));
            Path portDir = workerLogRoot.resolve("topoA").resolve("1111");
            Files.writeString(portDir.resolve("worker.log.1"), "rotated content");
            try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(portDir.resolve("worker.log.2.gz")))) {
                out.write("compressed content".getBytes(StandardCharsets.UTF_8));
            }

            LogviewerLogPageHandler handler = createHandlerAllowingOnlyTopoA(rootPath.getFile().toPath().resolve("logs"),
                mock(WorkerLogs.class));

            final Response rotated = handler.logPage("topoA/1111/worker.log.1", null, 100, null, "user");
            final Response compressed = handler.logPage("topoA/1111/worker.log.2.gz", null, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(rotated.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) rotated.getEntity(), containsString("rotated content"));
            assertThat(compressed.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) compressed.getEntity(), containsString("compressed content"));
        }
    }

    @Test
    public void testDaemonLogPageFollowsSymbolicLink() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path daemonLogRoot = rootPath.getFile().toPath().resolve("logs");
            createWorkerLogTree(daemonLogRoot);
            Path realLog = Files.writeString(daemonLogRoot.resolve("nimbus.log.real"), "nimbus content");
            Files.createSymbolicLink(daemonLogRoot.resolve("nimbus.log"), realLog);

            ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
            when(resourceAuthorizer.isUserAllowedToAccessDaemonFile(anyString())).thenReturn(true);
            LogviewerLogPageHandler handler = new LogviewerLogPageHandler(daemonLogRoot.resolve("workers-artifacts").toString(),
                daemonLogRoot.toString(), mock(WorkerLogs.class), resourceAuthorizer, new StormMetricsRegistry());

            final Response returned = handler.daemonLogPage("nimbus.log", 0, 100, null, "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(returned.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) returned.getEntity(), containsString("nimbus content"));
        }
    }

    /**
     * Creates topoA/1111/worker.log and topoB/1111/worker.log with distinct content below the worker log root.
     *
     * @return the worker log root
     */
    private Path createWorkerLogTree(Path daemonLogRoot) throws IOException {
        Path workerLogRoot = daemonLogRoot.resolve("workers-artifacts");
        Path topoAFile = workerLogRoot.resolve("topoA").resolve("1111").resolve("worker.log");
        Path topoBFile = workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.log");
        Files.createDirectories(topoAFile.getParent());
        Files.createDirectories(topoBFile.getParent());
        Files.writeString(topoAFile, "topoA content");
        Files.writeString(topoBFile, "topoB content");
        return workerLogRoot;
    }

    private LogviewerLogPageHandler createHandlerAllowingOnlyTopoA(Path daemonLogRoot, WorkerLogs workerLogs) {
        //Models per topology access: the user may only read the logs of topoA.
        ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
        when(resourceAuthorizer.isUserAllowedToAccessFile(anyString(), startsWith("topoA" + File.separator))).thenReturn(true);
        return new LogviewerLogPageHandler(daemonLogRoot.resolve("workers-artifacts").toString(), daemonLogRoot.toString(),
            workerLogs, resourceAuthorizer, new StormMetricsRegistry());
    }

    private LogviewerLogPageHandler createHandlerForTraversalTests(Path rootPath) throws IOException {
        return createHandlerForTraversalTests(rootPath, new ResourceAuthorizer(Utils.readStormConfig()));
    }

    private LogviewerLogPageHandler createHandlerForTraversalTests(Path rootPath, ResourceAuthorizer resourceAuthorizer)
            throws IOException {
        Path daemonLogRoot = rootPath.resolve("logs");
        Path fileOutsideDaemonRoot = rootPath.resolve("evil.sh");
        Path daemonFile = daemonLogRoot.resolve("nimbus.log");
        Path workerLogRoot = daemonLogRoot.resolve("workers-artifacts");
        Path topoA = workerLogRoot.resolve("topoA");
        Path file1 = topoA.resolve("1111").resolve("worker.log");
        Path file2 = topoA.resolve("2222").resolve("worker.log");
        Path file3 = workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.log");

        Files.createDirectories(file1.getParent());
        Files.createDirectories(file2.getParent());
        Files.createDirectories(file3.getParent());
        Files.createFile(file1);
        Files.createFile(file2);
        Files.createFile(file3);
        Files.createFile(fileOutsideDaemonRoot);
        Files.createFile(daemonFile);

        Map<String, Object> stormConf = Utils.readStormConfig();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        return new LogviewerLogPageHandler(workerLogRoot.toString(), daemonLogRoot.toString(),
            new WorkerLogs(stormConf, workerLogRoot, metricsRegistry), resourceAuthorizer, metricsRegistry);
    }
}
