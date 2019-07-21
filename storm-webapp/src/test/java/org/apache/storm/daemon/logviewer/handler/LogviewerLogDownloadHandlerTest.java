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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

public class LogviewerLogDownloadHandlerTest {

    @Test
    public void testDownloadLogFile() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerLogDownloadHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.downloadLogFile("host", "topoA/1111/worker.log", "user");
            Response topoBResponse = handler.downloadLogFile("host", "topoB/1111/worker.log", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(topoAResponse.getEntity(), not(nullValue()));
            String topoAContentDisposition = topoAResponse.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);
            assertThat(topoAContentDisposition, containsString("host-topoA-1111-worker.log"));
            assertThat(topoBResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(topoBResponse.getEntity(), not(nullValue()));
            String topoBContentDisposition = topoBResponse.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);
            assertThat(topoBContentDisposition, containsString("host-topoB-1111-worker.log"));
        }
    }

    @Test
    public void testDownloadLogFileTraversal() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerLogDownloadHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response topoAResponse = handler.downloadLogFile("host","../nimbus.log", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(topoAResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDaemonLogFile() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerLogDownloadHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response response = handler.downloadDaemonLogFile("host","nimbus.log", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
            assertThat(response.getEntity(), not(nullValue()));
            String contentDisposition = response.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);
            assertThat(contentDisposition, containsString("host-nimbus.log"));
        }
    }

    @Test
    public void testDownloadDaemonLogFilePathIntoWorkerLogs() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerLogDownloadHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response response = handler.downloadDaemonLogFile("host","workers-artifacts/topoA/1111/worker.log", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    @Test
    public void testDownloadDaemonLogFilePathOutsideLogRoot() throws IOException {
        try (TmpPath rootPath = new TmpPath()) {

            LogviewerLogDownloadHandler handler = createHandlerTraversalTests(rootPath.getFile().toPath());

            Response response = handler.downloadDaemonLogFile("host","../evil.sh", "user");

            Utils.forceDelete(rootPath.toString());

            assertThat(response.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));
        }
    }

    private LogviewerLogDownloadHandler createHandlerTraversalTests(Path rootPath) throws IOException {
        Path daemonLogRoot = rootPath.resolve("logs");
        Path fileOutsideDaemonRoot = rootPath.resolve("evil.sh");
        Path workerLogRoot = daemonLogRoot.resolve("workers-artifacts");
        Path daemonFile = daemonLogRoot.resolve("nimbus.log");
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
        return new LogviewerLogDownloadHandler(workerLogRoot.toString(), daemonLogRoot.toString(),
            new WorkerLogs(stormConf, workerLogRoot, metricsRegistry), new ResourceAuthorizer(stormConf), metricsRegistry);
    }

}
