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

package org.apache.storm.daemon.logviewer.handler;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.storm.daemon.logviewer.testsupport.SymbolicLinks;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.logviewer.utils.LogFilePaths;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class LogviewerProfileHandlerTest {

    @Test
    public void testListDumpFiles() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.listDumpFiles("topoA", "localhost:1111", "user");
            Response topoBResponse = handler.listDumpFiles("topoB", "localhost:1111", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
            String contentA = (String) topoAResponse.getEntity();
            assertThat(contentA, containsString("worker.jfr"));
            assertThat(contentA, not(containsString("worker.bin")));
            assertThat(contentA, not(containsString("worker.txt")));
            String contentB = (String) topoBResponse.getEntity();
            assertThat(contentB, containsString("worker.txt"));
            assertThat(contentB, not(containsString("worker.jfr")));
            assertThat(contentB, not(containsString("worker.bin")));
        }
    }

    @Test
    public void testListDumpFilesTraversalInTopoId() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response response = handler.listDumpFiles("../../", "localhost:logs", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testListDumpFilesTraversalInPort() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response response = handler.listDumpFiles("../", "localhost:../logs", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDumpFile() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.downloadDumpFile("topoA", "localhost:1111", "worker.jfr", "user");
            Response topoBResponse = handler.downloadDumpFile("topoB", "localhost:1111", "worker.txt", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(topoAResponse.getEntity(), not(nullValue()));
            String topoAContentDisposition = topoAResponse.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);
            assertThat(topoAContentDisposition, containsString("localhost-topoA-1111-worker.jfr"));
            assertThat(topoBResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(topoBResponse.getEntity(), not(nullValue()));
            String topoBContentDisposition = topoBResponse.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);
            assertThat(topoBContentDisposition, containsString("localhost-topoB-1111-worker.txt"));
        }
    }

    @Test
    public void testDownloadDumpFileTraversalInTopoId() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.downloadDumpFile("../../", "localhost:logs", "daemon-dump.bin", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDumpFileTraversalInPort() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerProfileHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.downloadDumpFile("../", "localhost:../logs", "daemon-dump.bin", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDumpFileDoesNotFollowSymbolicLink() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            SymbolicLinks.create(workerLogRoot.resolve("topoA").resolve("1111").resolve("other.jfr"),
                workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.jfr"));
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response response = handler.downloadDumpFile("topoA", "localhost:1111", "other.jfr", "user");
            int status = response.getStatus();
            String content = status == Response.Status.OK.getStatusCode() ? readEntity(response) : null;

            Utils.forceDelete(rootPath.toString());

            assertThat("served content: " + content, status, is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testListDumpFilesDoesNotListSymbolicLinks() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            SymbolicLinks.create(workerLogRoot.resolve("topoA").resolve("1111").resolve("other.jfr"),
                workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.jfr"));
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response response = handler.listDumpFiles("topoA", "localhost:1111", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
            String content = (String) response.getEntity();
            assertThat(content, containsString("worker.jfr"));
            assertThat(content, not(containsString("other.jfr")));
        }
    }

    @Test
    public void testListDumpFilesDoesNotFollowSymbolicPortDir() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            Path outside = rootPath.getFile().toPath().resolve("outside");
            Files.createDirectories(outside);
            Files.writeString(outside.resolve("secret.jfr"), "outside jfr");
            //A topology id containing a separator would name a directory below the port directory, which the worker controls
            SymbolicLinks.create(workerLogRoot.resolve("topoA").resolve("1111").resolve("123"), outside);
            SymbolicLinks.create(workerLogRoot.resolve("topoA").resolve("2222"), workerLogRoot.resolve("topoB").resolve("1111"));
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response belowPortDir = handler.listDumpFiles("topoA" + File.separator + "1111", "localhost:123", "user");
            Response linkedPortDir = handler.listDumpFiles("topoA", "localhost:2222", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat((String) belowPortDir.getEntity(), belowPortDir.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat((String) linkedPortDir.getEntity(), linkedPortDir.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDumpFileWithIpv6Host() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response list = handler.listDumpFiles("topoA", "[::1]:1111", "user");
            Response response = handler.downloadDumpFile("topoA", "[::1]:1111", "worker.jfr", "user");
            String content = readEntity(response);

            Utils.forceDelete(rootPath.toString());

            assertThat(list.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat((String) list.getEntity(), containsString("worker.jfr"));
            assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(response.getHeaderString(HttpHeaders.CONTENT_DISPOSITION), containsString("[::1]-topoA-1111-worker.jfr"));
            assertThat(content, is("TopoA jfr"));
        }
    }

    @Test
    public void testDownloadDumpFileUnderSymbolicLinkRoot() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path realWorkerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("real-workers-artifacts"));
            Path linkedWorkerLogRoot = rootPath.getFile().toPath().resolve("workers-artifacts");
            SymbolicLinks.create(linkedWorkerLogRoot, realWorkerLogRoot);
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(linkedWorkerLogRoot);

            Response response = handler.downloadDumpFile("topoA", "localhost:1111", "worker.jfr", "user");
            int status = response.getStatus();
            String content = status == Response.Status.OK.getStatusCode() ? readEntity(response) : null;

            Utils.forceDelete(rootPath.toString());

            assertThat(status, is(Response.Status.OK.getStatusCode()));
            assertThat(content, is("TopoA jfr"));
        }
    }

    @Test
    public void testDownloadDumpFileReplacedBySymbolicLinkAfterCheckIsNotFound() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            SymbolicLinks.create(workerLogRoot.resolve("topoA").resolve("1111").resolve("other.jfr"),
                workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.jfr"));
            ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
            when(resourceAuthorizer.isUserAllowedToAccessFile(anyString(), startsWith("topoA" + File.separator))).thenReturn(true);
            StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
            LogviewerProfileHandler handler = new LogviewerProfileHandler(workerLogRoot.toString(), resourceAuthorizer, metricsRegistry);

            Response response;
            try (MockedStatic<LogFilePaths> ignored = LogviewerLogDownloadHandlerTest.passWorkerFileCheck()) {
                response = handler.downloadDumpFile("topoA", "localhost:1111", "other.jfr", "user");
            }

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_DOWNLOAD_EXCEPTIONS).getCount(), is(0L));
        }
    }

    @Test
    public void testDownloadDumpFileRejectsFileNameWithSeveralElements() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            Path nested = workerLogRoot.resolve("topoA").resolve("1111").resolve("sub").resolve("worker.jfr");
            Files.createDirectories(nested.getParent());
            Files.writeString(nested, "nested jfr");
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response nestedResponse = handler.downloadDumpFile("topoA", "localhost:1111", "sub/worker.jfr", "user");
            //With an empty port, a two element file name would point at a direct child of another port directory
            Response emptyPortResponse = handler.downloadDumpFile("topoA", "localhost::1111", "1111/worker.jfr", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(nestedResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(emptyPortResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDumpFileRemovedAfterCheckIsNotFound() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
            when(resourceAuthorizer.isUserAllowedToAccessFile(anyString(), startsWith("topoA" + File.separator))).thenReturn(true);
            StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
            LogviewerProfileHandler handler = new LogviewerProfileHandler(workerLogRoot.toString(), resourceAuthorizer, metricsRegistry);

            Response response;
            try (MockedStatic<LogFilePaths> ignored = LogviewerLogDownloadHandlerTest.removeFileBeforeOpen()) {
                response = handler.downloadDumpFile("topoA", "localhost:1111", "worker.jfr", "user");
            }

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_DOWNLOAD_EXCEPTIONS).getCount(), is(0L));
        }
    }

    @Test
    public void testDumpEndpointsRejectPortThatIsNotANumber() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            Files.createDirectories(workerLogRoot.resolve("topoA").resolve("abc"));
            Files.writeString(workerLogRoot.resolve("topoA").resolve("abc").resolve("worker.jfr"), "abc jfr");
            Files.createDirectories(workerLogRoot.resolve("topoA").resolve("-1111"));
            Files.writeString(workerLogRoot.resolve("topoA").resolve("-1111").resolve("worker.jfr"), "negative jfr");
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response listNonNumeric = handler.listDumpFiles("topoA", "localhost:abc", "user");
            Response downloadNonNumeric = handler.downloadDumpFile("topoA", "localhost:abc", "worker.jfr", "user");
            Response listNegative = handler.listDumpFiles("topoA", "localhost:-1111", "user");
            Response downloadNegative = handler.downloadDumpFile("topoA", "localhost:-1111", "worker.jfr", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(listNonNumeric.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(downloadNonNumeric.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(listNegative.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(downloadNegative.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDumpEndpointsRejectEmptyPort() throws Exception {
        try (TmpPath rootPath = new TmpPath()) {
            Path workerLogRoot = createDumpFileTree(rootPath.getFile().toPath().resolve("workers-artifacts"));
            LogviewerProfileHandler handler = createHandlerAllowingOnlyTopoA(workerLogRoot);

            Response listEmpty = handler.listDumpFiles("topoA", "localhost:", "user");
            Response downloadEmpty = handler.downloadDumpFile("topoA", "localhost:", "worker.jfr", "user");
            //The port is the part after the last ':', so an IPv6 host without a port has an empty port
            Response listEmptyWithSuffix = handler.listDumpFiles("topoA", "[::1]:", "user");
            Response downloadEmptyWithSuffix = handler.downloadDumpFile("topoA", "[::1]:", "worker.jfr", "user");
            Response listNoPort = handler.listDumpFiles("topoA", "localhost", "user");
            Response downloadNoPort = handler.downloadDumpFile("topoA", "localhost", "worker.jfr", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(listEmpty.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(downloadEmpty.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(listEmptyWithSuffix.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(downloadEmptyWithSuffix.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(listNoPort.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
            assertThat(downloadNoPort.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    /**
     * Creates topoA/1111/worker.jfr and topoB/1111/worker.jfr with distinct content below the worker log root.
     *
     * @return the worker log root
     */
    private Path createDumpFileTree(Path workerLogRoot) throws IOException {
        Path topoAFile = workerLogRoot.resolve("topoA").resolve("1111").resolve("worker.jfr");
        Path topoBFile = workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.jfr");
        Files.createDirectories(topoAFile.getParent());
        Files.createDirectories(topoBFile.getParent());
        Files.writeString(topoAFile, "TopoA jfr");
        Files.writeString(topoBFile, "TopoB jfr");
        return workerLogRoot;
    }

    private LogviewerProfileHandler createHandlerAllowingOnlyTopoA(Path workerLogRoot) {
        //Models per topology access: the user may only read the files of topoA.
        ResourceAuthorizer resourceAuthorizer = mock(ResourceAuthorizer.class);
        when(resourceAuthorizer.isUserAllowedToAccessFile(anyString(), startsWith("topoA" + File.separator))).thenReturn(true);
        return new LogviewerProfileHandler(workerLogRoot.toString(), resourceAuthorizer, new StormMetricsRegistry());
    }

    private String readEntity(Response response) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ((StreamingOutput) response.getEntity()).write(out);
        return out.toString(StandardCharsets.UTF_8);
    }

    private LogviewerProfileHandler createHandlerTraversalTests(Path rootPath) throws IOException {
        Path daemonLogRoot = rootPath.resolve("logs");
        Path fileOutsideDaemonRoot = rootPath.resolve("evil.bin");
        Path workerLogRoot = daemonLogRoot.resolve("workers-artifacts");
        Path daemonFile = daemonLogRoot.resolve("daemon-dump.bin");
        Path topoA = workerLogRoot.resolve("topoA");
        Path file1 = topoA.resolve("1111").resolve("worker.jfr");
        Path file2 = topoA.resolve("2222").resolve("worker.bin");
        Path file3 = workerLogRoot.resolve("topoB").resolve("1111").resolve("worker.txt");

        Files.createDirectories(file1.getParent());
        Files.createDirectories(file2.getParent());
        Files.createDirectories(file3.getParent());
        Files.write(file1, "TopoA jfr".getBytes(StandardCharsets.UTF_8));
        Files.write(file3, "TopoB txt".getBytes(StandardCharsets.UTF_8));
        Files.createFile(file2);
        Files.createFile(fileOutsideDaemonRoot);
        Files.createFile(daemonFile);

        Map<String, Object> stormConf = Utils.readStormConfig();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        return new LogviewerProfileHandler(workerLogRoot.toString(), new ResourceAuthorizer(stormConf), metricsRegistry);
    }

}
