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

package org.apache.storm.daemon.logviewer.utils;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;

public class LogFileDownloader {

    private final String logRoot;
    private final String daemonLogRoot;
    private final ResourceAuthorizer resourceAuthorizer;

    public LogFileDownloader(String logRoot, String daemonLogRoot, ResourceAuthorizer resourceAuthorizer) {
        this.logRoot = logRoot;
        this.daemonLogRoot = daemonLogRoot;
        this.resourceAuthorizer = resourceAuthorizer;
    }

    public Response downloadFile(String fileName, String user, boolean isDaemon) throws IOException {
        String rootDir = isDaemon ? daemonLogRoot : logRoot;
        File file = new File(rootDir, fileName).getCanonicalFile();
        if (file.exists()) {
            if (isDaemon || resourceAuthorizer.isUserAllowedToAccessFile(fileName, user)) {
                return LogviewerResponseBuilder.buildDownloadFile(file);
            } else {
                return LogviewerResponseBuilder.buildResponseUnautohrizedUser(user);
            }
        } else {
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
    }

}
