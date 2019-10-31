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
import static org.junit.Assert.assertThat;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

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
