
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.logviewer.utils;

import org.apache.storm.metric.StormMetricsRegistry;

public class ExceptionMeterNames {

    //Operation level IO Exceptions
    public static final String NUM_FILE_OPEN_EXCEPTIONS = "logviewer:num-file-open-exceptions";
    public static final String NUM_FILE_READ_EXCEPTIONS = "logviewer:num-file-read-exceptions";
    public static final String NUM_FILE_REMOVAL_EXCEPTIONS = "logviewer:num-file-removal-exceptions";
    public static final String NUM_FILE_DOWNLOAD_EXCEPTIONS = "logviewer:num-file-download-exceptions";
    public static final String NUM_SET_PERMISSION_EXCEPTIONS = "logviewer:num-set-permission-exceptions";

    //Routine level
    public static final String NUM_CLEANUP_EXCEPTIONS = "logviewer:num-other-cleanup-exceptions";
    public static final String NUM_READ_LOG_EXCEPTIONS = "logviewer:num-read-log-exceptions";
    public static final String NUM_READ_DAEMON_LOG_EXCEPTIONS = "logviewer:num-read-daemon-log-exceptions";
    public static final String NUM_LIST_LOG_EXCEPTIONS = "logviewer:num-search-log-exceptions";
    public static final String NUM_LIST_DUMP_EXCEPTIONS = "logviewer:num-list-dump-files-exceptions";
    public static final String NUM_DOWNLOAD_DUMP_EXCEPTIONS = "logviewer:num-download-dump-exceptions";
    public static final String NUM_DOWNLOAD_LOG_EXCEPTIONS = "logviewer:num-download-log-exceptions";
    public static final String NUM_DOWNLOAD_DAEMON_LOG_EXCEPTIONS = "logviewer:num-download-daemon-log-exceptions";
    public static final String NUM_SEARCH_EXCEPTIONS = "logviewer:num-search-exceptions";

    /**
     * It may be helpful to register these meters up front, so they are output even if their values are zero.
     * @param registry The metrics registry.
     */
    public static void registerMeters(StormMetricsRegistry registry) {
        
        registry.registerMeter(NUM_FILE_OPEN_EXCEPTIONS);
        registry.registerMeter(NUM_FILE_READ_EXCEPTIONS);
        registry.registerMeter(NUM_FILE_REMOVAL_EXCEPTIONS);
        registry.registerMeter(NUM_FILE_DOWNLOAD_EXCEPTIONS);
        registry.registerMeter(NUM_SET_PERMISSION_EXCEPTIONS);
        registry.registerMeter(NUM_CLEANUP_EXCEPTIONS);
        registry.registerMeter(NUM_READ_LOG_EXCEPTIONS);
        registry.registerMeter(NUM_READ_DAEMON_LOG_EXCEPTIONS);
        registry.registerMeter(NUM_LIST_LOG_EXCEPTIONS);
        registry.registerMeter(NUM_LIST_DUMP_EXCEPTIONS);
        registry.registerMeter(NUM_DOWNLOAD_DUMP_EXCEPTIONS);
        registry.registerMeter(NUM_DOWNLOAD_LOG_EXCEPTIONS);
        registry.registerMeter(NUM_DOWNLOAD_DAEMON_LOG_EXCEPTIONS);
        registry.registerMeter(NUM_SEARCH_EXCEPTIONS);
    }
}
