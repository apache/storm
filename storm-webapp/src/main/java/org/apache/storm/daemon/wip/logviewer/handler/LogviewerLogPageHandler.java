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

package org.apache.storm.daemon.wip.logviewer.handler;

import j2html.TagCreator;
import j2html.tags.DomContent;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.DirectoryCleaner;
import org.apache.storm.daemon.utils.StreamUtil;
import org.apache.storm.daemon.utils.URLBuilder;
import org.apache.storm.daemon.wip.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.wip.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.wip.logviewer.utils.WorkerLogs;
import org.apache.storm.ui.InvalidRequestException;
import org.apache.storm.ui.UIHelpers;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.ws.rs.core.Response;

import static j2html.TagCreator.*;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;
import static org.apache.storm.daemon.wip.logviewer.LogviewerConstant.DEFAULT_BYTES_PER_PAGE;

public class LogviewerLogPageHandler {
    private final String logRoot;
    private final String daemonLogRoot;
    private final ResourceAuthorizer resourceAuthorizer;

    public LogviewerLogPageHandler(String logRoot, String daemonLogRoot,
                                   ResourceAuthorizer resourceAuthorizer) {
        this.logRoot = logRoot;
        this.daemonLogRoot = daemonLogRoot;
        this.resourceAuthorizer = resourceAuthorizer;
    }

    public Response listLogFiles(String user, Integer port, String topologyId, String callback, String origin) throws IOException {
        List<File> fileResults = null;
        if (topologyId == null) {
            if (port == null) {
                fileResults = WorkerLogs.getAllLogsForRootDir(new File(logRoot));
            } else {
                fileResults = new ArrayList<>();

                File[] logRootFiles = new File(logRoot).listFiles();
                if (logRootFiles != null) {
                    for (File topoDir : logRootFiles) {
                        File[] topoDirFiles = topoDir.listFiles();
                        if (topoDirFiles != null) {
                            for (File portDir : topoDirFiles) {
                                if (portDir.getName().equals(port.toString())) {
                                    fileResults.addAll(DirectoryCleaner.getFilesForDir(portDir));
                                }
                            }
                        }
                    }
                }
            }
        } else {
            if (port == null) {
                fileResults = new ArrayList<>();

                File topoDir = new File(logRoot + Utils.FILE_PATH_SEPARATOR + topologyId);
                if (topoDir.exists()) {
                    File[] topoDirFiles = topoDir.listFiles();
                    if (topoDirFiles != null) {
                        for (File portDir : topoDirFiles) {
                            fileResults.addAll(DirectoryCleaner.getFilesForDir(portDir));
                        }
                    }
                }

            } else {
                File portDir = ConfigUtils.getWorkerDirFromRoot(logRoot, topologyId, port);
                if (portDir.exists()) {
                    fileResults = DirectoryCleaner.getFilesForDir(portDir);
                }
            }
        }

        List<String> files;
        if (fileResults != null) {
            files = fileResults.stream()
                    .map(file -> WorkerLogs.getTopologyPortWorkerLog(file))
                    .sorted().collect(toList());
        } else {
            files = new ArrayList<>();
        }

        return LogviewerResponseBuilder.buildSuccessJsonResponse(files, callback, origin);
    }

    public Response logPage(String fileName, Integer start, Integer length, String grep, String user) throws IOException, InvalidRequestException {
        String rootDir = logRoot;
        if (resourceAuthorizer.isUserAllowedToAccessFile(fileName, user)) {
            File file = new File(rootDir, fileName).getCanonicalFile();
            String path = file.getCanonicalPath();
            boolean isZipFile = path.endsWith(".gz");
            File topoDir = file.getParentFile().getParentFile();

            if (file.exists() && new File(rootDir).getCanonicalFile().equals(topoDir.getParentFile())) {
                long fileLength = isZipFile ? ServerUtils.zipFileSize(file) : file.length();

                SortedSet<File> logFiles;
                try {
                    logFiles = Arrays.stream(topoDir.listFiles())
                            .flatMap(Unchecked.function(portDir -> DirectoryCleaner.getFilesForDir(portDir).stream()))
                            .filter(File::isFile)
                            .collect(toCollection(TreeSet::new));
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }

                List<String> filesStrWithoutFileParam = logFiles.stream().map(WorkerLogs::getTopologyPortWorkerLog)
                        .filter(fileStr -> !StringUtils.equals(fileName, fileStr)).collect(toList());

                List<String> reorderedFilesStr = new ArrayList<>();
                reorderedFilesStr.addAll(filesStrWithoutFileParam);
                reorderedFilesStr.add(fileName);

                length = length != null ? Math.min(10485760, length) : DEFAULT_BYTES_PER_PAGE;

                String logString;
                if (isTxtFile(fileName)) {
                    logString = escapeHtml(start != null ? pageFile(path, start, length) : pageFile(path, length));
                } else {
                    logString = escapeHtml("This is a binary file and cannot display! You may download the full file.");
                }

                start = start != null ? start : Long.valueOf(fileLength - length).intValue();

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
                    bodyContents.add(logFileSelectionForm(reorderedFilesStr, "log"));
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
            if (resourceAuthorizer.getLogUserGroupWhitelist(fileName) != null) {
                return LogviewerResponseBuilder.buildResponsePageNotFound();
            } else {
                return LogviewerResponseBuilder.buildResponseUnautohrizedUser(user);
            }
        }
    }

    public Response daemonLogPage(String fileName, Integer start, Integer length, String grep, String user) throws IOException, InvalidRequestException {
        String rootDir = daemonLogRoot;
        File file = new File(rootDir, fileName).getCanonicalFile();
        String path = file.getCanonicalPath();
        boolean isZipFile = path.endsWith(".gz");

        if (file.exists() && new File(rootDir).getCanonicalFile().equals(file.getParentFile())) {
            long fileLength = isZipFile ? ServerUtils.zipFileSize(file) : file.length();

            // all types of files included
            List<File> logFiles = Arrays.stream(new File(rootDir).listFiles())
                    .filter(File::isFile)
                    .collect(toList());

            List<String> filesStrWithoutFileParam = logFiles.stream()
                    .map(File::getName).filter(fName -> !StringUtils.equals(fileName, fName)).collect(toList());

            List<String> reorderedFilesStr = new ArrayList<>();
            reorderedFilesStr.addAll(filesStrWithoutFileParam);
            reorderedFilesStr.add(fileName);

            length = length != null ? Math.min(10485760, length) : DEFAULT_BYTES_PER_PAGE;

            String logString;
            if (isTxtFile(fileName)) {
                logString = escapeHtml(start != null ? pageFile(path, start, length) : pageFile(path, length));
            } else {
                logString = escapeHtml("This is a binary file and cannot display! You may download the full file.");
            }

            start = start != null ? start : Long.valueOf(fileLength - length).intValue();

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
                bodyContents.add(logFileSelectionForm(reorderedFilesStr, "daemonlog"));
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

    private DomContent logFileSelectionForm(List<String> logFiles, String type) {
        return form(
                dropDown("file", logFiles),
                input().withType("submit").withValue("Switch file")
        ).withAction(type).withId("list-of-files");
    }

    private DomContent dropDown(String name, List<String> logFiles) {
        List<DomContent> options = logFiles.stream().map(TagCreator::option).collect(toList());
        return select(options.toArray(new DomContent[]{})).withName(name).withId(name);
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
        int prevStart = Math.max(0, start - length);
        int nextStart = fileLength > 0 ? Math.min(Math.max(0, fileLength - length), start + length) : start + length;
        List<DomContent> btnLinks = new ArrayList<>();

        Map<String, Object> urlQueryParams = new HashMap<>();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", Math.max(0, start - length));
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(URLBuilder.build("/api/v1/" + type, urlQueryParams), "Prev", prevStart < start));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", 0);
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(URLBuilder.build("/api/v1/" + type, urlQueryParams), "First"));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(URLBuilder.build("/api/v1/" + type, urlQueryParams), "Last"));

        urlQueryParams.clear();
        urlQueryParams.put("file", fileName);
        urlQueryParams.put("start", Math.min(Math.max(0, fileLength - length), start + length));
        urlQueryParams.put("length", length);

        btnLinks.add(toButtonLink(URLBuilder.build("/api/v1/" + type, urlQueryParams), "Next", nextStart > start));

        return div(btnLinks.toArray(new DomContent[]{}));
    }

    private DomContent toButtonLink(String url, String text) {
        return toButtonLink(url, text, true);
    }

    private DomContent toButtonLink(String url, String text, boolean enabled) {
        return a(text).withHref(url).withClass("btn btn-default " + (enabled ? "enabled" : "disabled"));
    }

    private String pageFile(String path, Integer tail) throws IOException, InvalidRequestException {
        boolean isZipFile = path.endsWith(".gz");
        long fileLength = isZipFile ? ServerUtils.zipFileSize(new File(path)) : new File(path).length();
        long skip = fileLength - tail;
        return pageFile(path, Long.valueOf(skip).intValue(), tail);
    }

    private String pageFile(String path, Integer start, Integer length) throws IOException, InvalidRequestException {
        boolean isZipFile = path.endsWith(".gz");
        long fileLength = isZipFile ? ServerUtils.zipFileSize(new File(path)) : new File(path).length();

        try (InputStream input = isZipFile ? new GZIPInputStream(new FileInputStream(path)) : new FileInputStream(path);
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            if (start >= fileLength) {
                throw new InvalidRequestException("Cannot start past the end of the file");
            }
            if (start > 0) {
                StreamUtil.skipBytes(input, start);
            }

            byte[] buffer = new byte[1024];
            while (output.size() < length) {
                int size = input.read(buffer, 0, Math.min(1024, length - output.size()));
                if (size > 0) {
                    output.write(buffer, 0, size);
                } else {
                    break;
                }
            }

            return output.toString();
        }
    }

    private boolean isTxtFile(String fileName) {
        Pattern p = Pattern.compile("\\.(log.*|txt|yaml|pid)$");
        Matcher matcher = p.matcher(fileName);
        return matcher.find();
    }
}
