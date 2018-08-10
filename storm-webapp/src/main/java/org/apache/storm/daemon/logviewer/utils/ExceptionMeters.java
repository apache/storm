/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.daemon.logviewer.utils;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

public enum ExceptionMeters {
    //Operation level IO Exceptions
    NUM_FILE_OPEN_EXCEPTIONS("logviewer:num-file-open-exceptions"),
    NUM_FILE_READ_EXCEPTIONS("logviewer:num-file-read-exceptions"),
    NUM_FILE_REMOVAL_EXCEPTIONS("logviewer:num-file-removal-exceptions"),
    NUM_FILE_DOWNLOAD_EXCEPTIONS("logviewer:num-file-download-exceptions"),
    NUM_SET_PERMISSION_EXCEPTIONS("logviewer:num-set-permission-exceptions"),

    //Routine level
    NUM_CLEANUP_EXCEPTIONS("logviewer:num-other-cleanup-exceptions"),
    NUM_READ_LOG_EXCEPTIONS("logviewer:num-read-log-exceptions"),
    NUM_READ_DAEMON_LOG_EXCEPTIONS("logviewer:num-read-daemon-log-exceptions"),
    NUM_LIST_LOG_EXCEPTIONS("logviewer:num-search-log-exceptions"),
    NUM_LIST_DUMP_EXCEPTIONS("logviewer:num-list-dump-files-exceptions"),
    NUM_DOWNLOAD_DUMP_EXCEPTIONS("logviewer:num-download-dump-exceptions"),
    NUM_DOWNLOAD_LOG_EXCEPTIONS("logviewer:num-download-log-exceptions"),
    NUM_DOWNLOAD_DAEMON_LOG_EXCEPTIONS("logviewer:num-download-daemon-log-exceptions"),
    NUM_SEARCH_EXCEPTIONS("logviewer:num-search-exceptions");

    private static final Map<String, Metric> metrics = new HashMap<>();

    static {
        for (ExceptionMeters e : ExceptionMeters.values()) {
            metrics.put(e.name, e.meter);
        }
    }

    private final String name;
    private final Meter meter;

    public static Map<String, Metric> getMetrics() {
        return metrics;
    }

    ExceptionMeters(String name) {
        this.name = name;
        meter = new Meter();
    }

    public void mark() {
        this.meter.mark();
    }
}
