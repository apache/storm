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

import org.apache.storm.daemon.wip.logviewer.utils.LogFileDownloader;
import org.apache.storm.daemon.wip.logviewer.utils.ResourceAuthorizer;

import javax.ws.rs.core.Response;
import java.io.IOException;

public class LogviewerLogDownloadHandler {

    private final LogFileDownloader logFileDownloadHelper;

    public LogviewerLogDownloadHandler(String logRoot, String daemonLogRoot, ResourceAuthorizer resourceAuthorizer) {
        this.logFileDownloadHelper = new LogFileDownloader(logRoot, daemonLogRoot, resourceAuthorizer);
    }

    public Response downloadLogFile(String fileName, String user) throws IOException {
        return logFileDownloadHelper.downloadFile(fileName, user, false);
    }

    public Response downloadDaemonLogFile(String fileName, String user) throws IOException {
        return logFileDownloadHelper.downloadFile(fileName, user, true);
    }

}
