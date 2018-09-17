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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import java.io.File;
import java.io.IOException;

import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.apache.storm.metric.StormMetricsRegistry;


public class LogFileDownloader {
    private final Histogram fileDownloadSizeDistMb;
    private final Meter numFileDownloadExceptions;
    private final String logRoot;
    private final String daemonLogRoot;
    private final ResourceAuthorizer resourceAuthorizer;

    /**
     * Constructor.
     *
     * @param logRoot root worker log directory
     * @param daemonLogRoot root daemon log directory
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     * @param metricsRegistry The logviewer metrics registry
     */
    public LogFileDownloader(String logRoot, String daemonLogRoot, ResourceAuthorizer resourceAuthorizer,
        StormMetricsRegistry metricsRegistry) {
        this.logRoot = logRoot;
        this.daemonLogRoot = daemonLogRoot;
        this.resourceAuthorizer = resourceAuthorizer;
        this.fileDownloadSizeDistMb = metricsRegistry.registerHistogram("logviewer:download-file-size-rounded-MB");
        this.numFileDownloadExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_DOWNLOAD_EXCEPTIONS);
    }

    /**
     * Checks authorization for the log file and download.
     *
     * @param fileName file to download
     * @param user username
     * @param isDaemon true if the file is a daemon log, false if the file is an worker log
     * @return a Response which lets browsers download that file.
     */
    public Response downloadFile(String fileName, String user, boolean isDaemon) throws IOException {
        String rootDir = isDaemon ? daemonLogRoot : logRoot;
        File file = new File(rootDir, fileName).getCanonicalFile();
        if (file.exists()) {
            if (isDaemon || resourceAuthorizer.isUserAllowedToAccessFile(user, fileName)) {
                fileDownloadSizeDistMb.update(Math.round((double) file.length() / FileUtils.ONE_MB));
                return LogviewerResponseBuilder.buildDownloadFile(file, numFileDownloadExceptions);
            } else {
                return LogviewerResponseBuilder.buildResponseUnauthorizedUser(user);
            }
        } else {
            return LogviewerResponseBuilder.buildResponsePageNotFound();
        }
    }

}
