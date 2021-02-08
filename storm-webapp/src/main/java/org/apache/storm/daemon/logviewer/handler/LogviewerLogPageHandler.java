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
import static j2html.TagCreator.div;
import static j2html.TagCreator.form;
import static j2html.TagCreator.h3;
import static j2html.TagCreator.head;
import static j2html.TagCreator.html;
import static j2html.TagCreator.input;
import static j2html.TagCreator.link;
import static j2html.TagCreator.option;
import static j2html.TagCreator.p;
import static j2html.TagCreator.pre;
import static j2html.TagCreator.select;
import static j2html.TagCreator.text;
import static j2html.TagCreator.title;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

import com.codahale.metrics.Meter;
import j2html.tags.DomContent;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.logviewer.LogviewerConstant;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.daemon.ui.InvalidRequestException;
import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.daemon.utils.StreamUtil;
import org.apache.storm.daemon.utils.UrlBuilder;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.jooq.lambda.Unchecked;

public class LogviewerLogPageHandler {
    private final Meter numPageRead;
    private final Meter numFileOpenExceptions;
    private final Meter numFileReadExceptions;
    private final Path logRoot;
    private final Path daemonLogRoot;
    private final WorkerLogs workerLogs;
    private final ResourceAuthorizer resourceAuthorizer;
    private final DirectoryCleaner directoryCleaner;

    /**
     * Constructor.
     *
     * @param logRoot root worker log directory
     * @param daemonLogRoot root daemon log directory
     * @param workerLogs {@link WorkerLogs}
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     * @param metricsRegistry The logviewer metrics registry
     */
    public LogviewerLogPageHandler(String logRoot, String daemonLogRoot,
                                   WorkerLogs workerLogs,
                                   ResourceAuthorizer resourceAuthorizer,
                                   StormMetricsRegistry metricsRegistry) {
        this.logRoot = Paths.get(logRoot).toAbsolutePath().normalize();
        this.daemonLogRoot = Paths.get(daemonLogRoot).toAbsolutePath().normalize();
        this.workerLogs = workerLogs;
        this.resourceAuthorizer = resourceAuthorizer;
        this.numPageRead = metricsRegistry.registerMeter("logviewer:num-page-read");
        this.numFileOpenExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_OPEN_EXCEPTIONS);
        this.numFileReadExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_READ_EXCEPTIONS);
        this.directoryCleaner = new DirectoryCleaner(metricsRegistry);
    }

    /**
     * Enumerate worker log files for given criteria.
     *
     * @param user username
     * @param port worker's port, null for all workers
     * @param topologyId topology ID, null for all topologies
     * @param callback callbackParameterName for JSONP
     * @param origin origin
     * @return list of worker logs for given criteria
     */
    public Response listLogFiles(String user, Integer port, String topologyId, String callback, String origin) throws IOException {
        List<Path> fileResults = null;
        if (topologyId == null) {
            if (port == null) {
                fileResults = workerLogs.getAllLogsForRootDir();
            } else {
                fileResults = new ArrayList<>();

                File[] logRootFiles = logRoot.toFile().listFiles();
                if (logRootFiles != null) {
                    for (File topoDir : logRootFiles) {
                        File[] topoDirFiles = topoDir.listFiles();
                        if (topoDirFiles != null) {
                            for (File portDir : topoDirFiles) {
                                if (portDir.getName().equals(port.toString())) {
                                    fileResults.addAll(directoryCleaner.getFilesForDir(portDir.toPath()));
                                }
                            }
                        }
                    }
                }
            }
        } else {
            if (port == null) {
                fileResults = new ArrayList<>();

                Path topoDir = logRoot.resolve(topologyId).toAbsolutePath().normalize();
                if (!topoDir.startsWith(logRoot)) {
                    return LogviewerResponseBuilder.buildSuccessJsonResponse(Collections.emptyList(), callback, origin);
                }
                if (topoDir.toFile().exists()) {
                    File[] topoDirFiles = topoDir.toFile().listFiles();
                    if (topoDirFiles != null) {
                        for (File portDir : topoDirFiles) {
                            fileResults.addAll(directoryCleaner.getFilesForDir(portDir.toPath()));
                        }
                    }
                }

            } else {
                File portDir = ConfigUtils.getWorkerDirFromRoot(logRoot.toString(), topologyId, port).getCanonicalFile();
                if (!portDir.getPath().startsWith(logRoot.toString())) {
                    return LogviewerResponseBuilder.buildSuccessJsonResponse(Collections.emptyList(), callback, origin);
                }
                if (portDir.exists()) {
                    fileResults = directoryCleaner.getFilesForDir(portDir.toPath());
                }
            }
        }

        List<String> files;
        if (fileResults != null) {
            files = fileResults.stream()
                    .map(WorkerLogs::getTopologyPortWorkerLog)
                    .sorted().collect(toList());
        } else {
            files = new ArrayList<>();
        }

        return LogviewerResponseBuilder.buildSuccessJsonResponse(files, callback, origin);
    }

    /**
     * Provides a worker log file to view, starting from the specified position
     * or default starting position of the most recent page.
     *
     * @param fileName file to view
     * @param start start offset, or null if the most recent page is desired
     * @param length length to read in this page, or null if default page length is desired
     * @param grep search string if request is a result of the search, can be null
     * @param user username
     * @return HTML view page of worker log
     */
    public Response logPage(String fileName, Integer start, Integer length, String grep, String user)
            throws IOException, InvalidRequestException {
        Path rawFile = logRoot.resolve(fileName);
        Path absFile = rawFile.toAbsolutePath().normalize();
        if (!absFile.startsWith(logRoot) || !rawFile.normalize().toString().equals(rawFile.toString())) {
            //Ensure filename doesn't contain ../ parts 
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
        
        if (resourceAuthorizer.isUserAllowedToAccessFile(user, fileName)) {
            workerLogs.setLogFilePermission(fileName);

            Path topoDir = absFile.getParent().getParent();
            if (absFile.toFile().exists()) {
                SortedSet<Path> logFiles;
                try {
                    logFiles = Arrays.stream(topoDir.toFile().listFiles())
                        .flatMap(Unchecked.function(portDir -> directoryCleaner.getFilesForDir(portDir.toPath()).stream()))
                        .filter(Files::isRegularFile)
                            .collect(toCollection(TreeSet::new));
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }

                List<String> reorderedFilesStr = logFiles.stream()
                        .map(WorkerLogs::getTopologyPortWorkerLog)
                        .filter(fileStr -> !StringUtils.equals(fileName, fileStr))
                        .collect(toList());
                reorderedFilesStr.add(fileName);

                length = length != null ? Math.min(10485760, length) : LogviewerConstant.DEFAULT_BYTES_PER_PAGE;
                final boolean isZipFile = absFile.getFileName().toString().endsWith(".gz");
                long fileLength = getFileLength(absFile.toFile(), isZipFile);
                if (start == null) {
                    start = Long.valueOf(fileLength - length).intValue();
                }

                String logString = isTxtFile(fileName) ? escapeHtml(pageFile(absFile.toString(), isZipFile, fileLength, start, length)) :
                    escapeHtml("This is a binary file and cannot display! You may download the full file.");

                List<DomContent> bodyContents = new ArrayList<>();
                if (StringUtils.isNotEmpty(grep)) {
                    String matchedString = String.join("\n", Arrays.stream(logString.split("\n"))
                            .filter(str -> str.contains(grep)).collect(toList()));
                    bodyContents.add(pre(matchedString).withId("logContent"));
                } else {
                    DomContent pagerData = null;
                    if (isTxtFile(fileName)) {
                        pagerData = pagerLinks(fileName, start, length, Long.valueOf(fileLength).intValue(), "log");
                    }

                    bodyContents.add(searchFileForm(fileName, "no"));
                    // list all files for this topology
                    bodyContents.add(logFileSelectionForm(reorderedFilesStr, fileName, "log"));
                    if (pagerData != null) {
                        bodyContents.add(pagerData);
                    }
                    bodyContents.add(downloadLink(fileName));
                    bodyContents.add(pre(logString).withClass("logContent"));
                    if (pagerData != null) {
                        bodyContents.add(pagerData);
                    }
                }

                String content = logTemplate(bodyContents, fileName, user).render();
                return LogviewerResponseBuilder.buildSuccessHtmlResponse(content);
            } else {
                return LogviewerResponseBuilder.buildResponsePageNotFound();
            }
        } else {
            if (resourceAuthorizer.getLogUserGroupWhitelist(fileName) == null) {
                return LogviewerResponseBuilder.buildResponsePageNotFound();
            } else {
                return LogviewerResponseBuilder.buildResponseUnauthorizedUser(user);
            }
        }
    }

    /**
     * Provides a daemon log file to view.
     *
     * @param fileName file to view
     * @param start start offset, or null if the most recent page is desired
     * @param length length to read in this page, or null if default page length is desired
     * @param grep search string if request is a result of the search, can be null
     * @param user username
     * @return HTML view page of daemon log
     */
    public Response daemonLogPage(String fileName, Integer start, Integer length, String grep, String user)
            throws IOException, InvalidRequestException {
        Path file = daemonLogRoot.resolve(fileName).toAbsolutePath().normalize();
        if (!file.startsWith(daemonLogRoot) || Paths.get(fileName).getNameCount() != 1) {
            //Prevent fileName from pathing into worker logs, or outside daemon log root 
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }

        if (file.toFile().exists()) {
            // all types of files included
            List<File> logFiles = Arrays.stream(daemonLogRoot.toFile().listFiles())
                    .filter(File::isFile)
                    .collect(toList());

            List<String> reorderedFilesStr = logFiles.stream()
                    .map(File::getName)
                    .filter(fName -> !StringUtils.equals(fileName, fName))
                    .collect(toList());
            reorderedFilesStr.add(fileName);

            length = length != null ? Math.min(10485760, length) : LogviewerConstant.DEFAULT_BYTES_PER_PAGE;
            final boolean isZipFile = file.getFileName().toString().endsWith(".gz");
            long fileLength = getFileLength(file.toFile(), isZipFile);
            if (start == null) {
                start = Long.valueOf(fileLength - length).intValue();
            }

            String logString = isTxtFile(fileName) ? escapeHtml(pageFile(file.toString(), isZipFile, fileLength, start, length)) :
                    escapeHtml("This is a binary file and cannot display! You may download the full file.");

            List<DomContent> bodyContents = new ArrayList<>();
            if (StringUtils.isNotEmpty(grep)) {
                String matchedString = String.join("\n", Arrays.stream(logString.split("\n"))
                        .filter(str -> str.contains(grep)).collect(toList()));
                bodyContents.add(pre(matchedString).withId("logContent"));
            } else {
                DomContent pagerData = null;
                if (isTxtFile(fileName)) {
                    pagerData = pagerLinks(fileName, start, length, Long.valueOf(fileLength).intValue(), "daemonlog");
                }

                bodyContents.add(searchFileForm(fileName, "yes"));
                // list all daemon logs
                bodyContents.add(logFileSelectionForm(reorderedFilesStr, fileName, "daemonlog"));
                if (pagerData != null) {
                    bodyContents.add(pagerData);
                }
                bodyContents.add(daemonDownloadLink(fileName));
                bodyContents.add(pre(logString).withClass("logContent"));
                if (pagerData != null) {
                    bodyContents.add(pagerData);
                }
            }

            String content = logTemplate(bodyContents, fileName, user).render();
            return LogviewerResponseBuilder.buildSuccessHtmlResponse(content);
        } else {
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
    }

    private long getFileLength(File file, boolean isZipFile) throws IOException {
        try {
            return isZipFile ? ServerUtils.zipFileSize(file) : file.length();
        } catch (FileNotFoundException e) {
            numFileOpenExceptions.mark();
            throw e;
        } catch (IOException e) {
            numFileReadExceptions.mark();
            throw e;
        }
    }

    private DomContent logTemplate(List<DomContent> bodyContents, String fileName, String user) {
        List<DomContent> finalBodyContents = new ArrayList<>();

        if (StringUtils.isNotBlank(user)) {
            finalBodyContents.add(div(p("User: " + user)).withClass("ui-user"));
        }

        finalBodyContents.add(div(p("Note: the drop-list shows at most 1024 files for each worker directory.")).withClass("ui-note"));
        finalBodyContents.add(h3(escapeHtml(fileName)));
        finalBodyContents.addAll(bodyContents);

        return html(
                head(
                        title(escapeHtml(fileName) + " - Storm Log Viewer"),
                        link().withRel("stylesheet").withHref("/css/bootstrap-3.3.1.min.css"),
                        link().withRel("stylesheet").withHref("/css/jquery.dataTables.1.10.4.min.css"),
                        link().withRel("stylesheet").withHref("/css/style.css")
                ),
                body(
                        finalBodyContents.toArray(new DomContent[]{})
                )
        );
    }

    private DomContent downloadLink(String fileName) {
        return p(linkTo(UIHelpers.urlFormat("/api/v1/download?file=%s", fileName), "Download Full File"));
    }

    private DomContent daemonDownloadLink(String fileName) {
        return p(linkTo(UIHelpers.urlFormat("/api/v1/daemondownload?file=%s", fileName), "Download Full File"));
    }

    private DomContent linkTo(String url, String content) {
        return a(content).withHref(url);
    }

    private DomContent logFileSelectionForm(List<String> logFiles, String selectedFile, String type) {
        return form(
                dropDown("file", logFiles, selectedFile),
                input().withType("submit").withValue("Switch file")
        ).withAction(type).withId("list-of-files");
    }

    private DomContent dropDown(String name, List<String> logFiles, String selectedFile) {
        List<DomContent> options = logFiles.stream()
                .map(file -> option(file).condAttr(file.equals(selectedFile), "selected", "selected"))
                .collect(toList());
        return select(options.toArray(new DomContent[]{})).withName(name).withId(name).withValue(selectedFile);
    }

    private DomContent searchFileForm(String fileName, String isDaemonValue) {
        return form(
                text("search this file:"),
                input().withType("text").withName("search"),
                input().withType("hidden").withName("is-daemon").withValue(isDaemonValue),
                input().withType("hidden").withName("file").withValue(fileName),
                input().withType("submit").withValue("Search")
        ).withAction("/logviewer_search.html").withId("search-box");
    }

    private DomContent pagerLinks(String fileName, Integer start, Integer length, Integer fileLength, String type) {
        Map<String, Object> urlQueryParams = new HashMap<>();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", Math.max(0, start - length));
        urlQueryParams.put("length", length);

        List<DomContent> btnLinks = new ArrayList<>();

        int prevStart = Math.max(0, start - length);
        btnLinks.add(toButtonLink(UrlBuilder.build("/api/v1/" + type, urlQueryParams), "Prev", prevStart < start));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", 0);
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(UrlBuilder.build("/api/v1/" + type, urlQueryParams), "First"));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(UrlBuilder.build("/api/v1/" + type, urlQueryParams), "Last"));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", Math.min(Math.max(0, fileLength - length), start + length));
        urlQueryParams.put("length", length);

        int nextStart = fileLength > 0 ? Math.min(Math.max(0, fileLength - length), start + length) : start + length;
        btnLinks.add(toButtonLink(UrlBuilder.build("/api/v1/" + type, urlQueryParams), "Next", nextStart > start));

        return div(btnLinks.toArray(new DomContent[]{}));
    }

    private DomContent toButtonLink(String url, String text) {
        return toButtonLink(url, text, true);
    }

    private DomContent toButtonLink(String url, String text, boolean enabled) {
        return a(text).withHref(url).withClass("btn btn-default " + (enabled ? "enabled" : "disabled"));
    }

    private String pageFile(String path, boolean isZipFile, long fileLength, Integer start, Integer readLength)
        throws IOException, InvalidRequestException {
        try (InputStream input = isZipFile ? new GZIPInputStream(new FileInputStream(path)) : new FileInputStream(path);
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            if (start >= fileLength) {
                throw new InvalidRequestException("Cannot start past the end of the file");
            }
            if (start > 0) {
                StreamUtil.skipBytes(input, start);
            }

            byte[] buffer = new byte[1024];
            while (output.size() < readLength) {
                int size = input.read(buffer, 0, Math.min(1024, readLength - output.size()));
                if (size > 0) {
                    output.write(buffer, 0, size);
                } else {
                    break;
                }
            }

            numPageRead.mark();
            return output.toString();
        } catch (FileNotFoundException e) {
            numFileOpenExceptions.mark();
            throw e;
        } catch (IOException e) {
            numFileReadExceptions.mark();
            throw e;
        }
    }

    private boolean isTxtFile(String fileName) {
        Pattern p = Pattern.compile("\\.(log.*|txt|yaml|pid)$");
        Matcher matcher = p.matcher(fileName);
        return matcher.find();
    }
}
