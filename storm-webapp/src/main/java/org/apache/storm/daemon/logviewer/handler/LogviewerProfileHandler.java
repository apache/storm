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

import static j2html.TagCreator.a;
import static j2html.TagCreator.body;
import static j2html.TagCreator.head;
import static j2html.TagCreator.html;
import static j2html.TagCreator.li;
import static j2html.TagCreator.link;
import static j2html.TagCreator.title;
import static j2html.TagCreator.ul;
import static java.util.stream.Collectors.toList;

import j2html.tags.DomContent;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.utils.ServerUtils;

public class LogviewerProfileHandler {

    public static final String WORKER_LOG_FILENAME = "worker.log";
    private final String logRoot;
    private final ResourceAuthorizer resourceAuthorizer;

    /**
     * Constructor.
     *
     * @param logRoot worker log root directory
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     */
    public LogviewerProfileHandler(String logRoot, ResourceAuthorizer resourceAuthorizer) {
        this.logRoot = logRoot;
        this.resourceAuthorizer = resourceAuthorizer;
    }

    /**
     * Enumerate dump (profile) files for given worker.
     *
     * @param topologyId topology ID
     * @param hostPort host and port of worker
     * @param user username
     * @return The HTML page representing list page of dump files
     */
    public Response listDumpFiles(String topologyId, String hostPort, String user) throws IOException {
        String portStr = hostPort.split(":")[1];
        File dir = new File(String.join(ServerUtils.FILE_PATH_SEPARATOR, logRoot, topologyId, portStr));

        if (dir.exists()) {
            String workerFileRelativePath = String.join(ServerUtils.FILE_PATH_SEPARATOR, topologyId, portStr, WORKER_LOG_FILENAME);
            if (resourceAuthorizer.isUserAllowedToAccessFile(user, workerFileRelativePath)) {
                String content = buildDumpFileListPage(topologyId, hostPort, dir);
                return LogviewerResponseBuilder.buildSuccessHtmlResponse(content);
            } else {
                return LogviewerResponseBuilder.buildResponseUnautohrizedUser(user);
            }
        } else {
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
    }

    /**
     * Download a dump file.
     *
     * @param topologyId topology ID
     * @param hostPort host and port of worker
     * @param fileName dump file name
     * @param user username
     * @return a Response which lets browsers download that file.
     * @see {@link org.apache.storm.daemon.logviewer.utils.LogFileDownloader#downloadFile(String, String, boolean)}
     */
    public Response downloadDumpFile(String topologyId, String hostPort, String fileName, String user) throws IOException {
        String portStr = hostPort.split(":")[1];
        File dir = new File(String.join(ServerUtils.FILE_PATH_SEPARATOR, logRoot, topologyId, portStr));
        File file = new File(dir, fileName);

        if (dir.exists() && file.exists()) {
            String workerFileRelativePath = String.join(ServerUtils.FILE_PATH_SEPARATOR, topologyId, portStr, WORKER_LOG_FILENAME);
            if (resourceAuthorizer.isUserAllowedToAccessFile(user, workerFileRelativePath)) {
                return LogviewerResponseBuilder.buildDownloadFile(file);
            } else {
                return LogviewerResponseBuilder.buildResponseUnautohrizedUser(user);
            }
        } else {
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
    }

    private String buildDumpFileListPage(String topologyId, String hostPort, File dir) throws IOException {
        List<DomContent> liTags = getProfilerDumpFiles(dir).stream()
                .map(file -> li(a(file).withHref("/api/v1/dumps/" + topologyId + "/" + hostPort + "/" + file)))
                .collect(toList());

        return html(
                head(
                        title("File Dumps - Storm Log Viewer"),
                        link().withRel("stylesheet").withHref("/css/bootstrap-3.3.1.min.css"),
                        link().withRel("stylesheet").withHref("/css/jquery.dataTables.1.10.4.min.css"),
                        link().withRel("stylesheet").withHref("/css/style.css")
                ),
                body(
                        ul(liTags.toArray(new DomContent[]{}))
                )
        ).render();
    }

    private List<String> getProfilerDumpFiles(File dir) throws IOException {
        List<File> filesForDir = DirectoryCleaner.getFilesForDir(dir);
        return filesForDir.stream().filter(file -> {
            String fileName = file.getName();
            return StringUtils.isNotEmpty(fileName)
                    && (fileName.endsWith(".txt") || fileName.endsWith(".jfr") || fileName.endsWith(".bin"));
        }).map(File::getName).collect(toList());
    }

}
