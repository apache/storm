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

import com.codahale.metrics.Meter;
import j2html.tags.DomContent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.metric.StormMetricsRegistry;

public class LogviewerProfileHandler {

    public static final String WORKER_LOG_FILENAME = "worker.log";

    private final Meter numFileDownloadExceptions;

    private final Path logRoot;
    private final ResourceAuthorizer resourceAuthorizer;
    private final DirectoryCleaner directoryCleaner;

    /**
     * Constructor.
     *
     * @param logRoot worker log root directory
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     * @param metricsRegistry The logviewer metrisc registry
     */
    public LogviewerProfileHandler(String logRoot, ResourceAuthorizer resourceAuthorizer, StormMetricsRegistry metricsRegistry) {
        this.logRoot = Paths.get(logRoot).toAbsolutePath().normalize();
        this.resourceAuthorizer = resourceAuthorizer;
        this.numFileDownloadExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_DOWNLOAD_EXCEPTIONS);
        this.directoryCleaner = new DirectoryCleaner(metricsRegistry);
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
        Path rawDir = logRoot.resolve(topologyId).resolve(portStr);
        Path absDir = rawDir.toAbsolutePath().normalize();
        if (!absDir.startsWith(logRoot) || !rawDir.normalize().toString().equals(rawDir.toString())) {
            //Ensure filename doesn't contain ../ parts 
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }

        if (absDir.toFile().exists()) {
            String workerFileRelativePath = String.join(File.separator, topologyId, portStr, WORKER_LOG_FILENAME);
            if (resourceAuthorizer.isUserAllowedToAccessFile(user, workerFileRelativePath)) {
                String content = buildDumpFileListPage(topologyId, hostPort, absDir.toFile());
                return LogviewerResponseBuilder.buildSuccessHtmlResponse(content);
            } else {
                return LogviewerResponseBuilder.buildResponseUnauthorizedUser(user);
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
     * @see {@link org.apache.storm.daemon.logviewer.utils.LogFileDownloader#downloadFile(String, String, String, boolean)}
     */
    public Response downloadDumpFile(String topologyId, String hostPort, String fileName, String user) throws IOException {
        String[] hostPortSplit = hostPort.split(":");
        String host = hostPortSplit[0];
        String portStr = hostPortSplit[1];
        Path rawFile = logRoot.resolve(topologyId).resolve(portStr).resolve(fileName);
        Path absFile = rawFile.toAbsolutePath().normalize();
        if (!absFile.startsWith(logRoot) || !rawFile.normalize().toString().equals(rawFile.toString())) {
            //Ensure filename doesn't contain ../ parts 
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }

        if (absFile.toFile().exists()) {
            String workerFileRelativePath = String.join(File.separator, topologyId, portStr, WORKER_LOG_FILENAME);
            if (resourceAuthorizer.isUserAllowedToAccessFile(user, workerFileRelativePath)) {
                String downloadedFileName = host + "-" + topologyId + "-" + portStr + "-" + absFile.getFileName();
                return LogviewerResponseBuilder.buildDownloadFile(downloadedFileName, absFile.toFile(), numFileDownloadExceptions);
            } else {
                return LogviewerResponseBuilder.buildResponseUnauthorizedUser(user);
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
        List<Path> filesForDir = directoryCleaner.getFilesForDir(dir.toPath());
        return filesForDir.stream()
            .map(path -> path.toFile())
            .filter(file -> {
                String fileName = file.getName();
                return StringUtils.isNotEmpty(fileName)
                    && (fileName.endsWith(".txt") || fileName.endsWith(".jfr") || fileName.endsWith(".bin"));
            }).map(File::getName).collect(toList());
    }

}
