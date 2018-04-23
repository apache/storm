/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor.error;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportErrorAndDie implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReportErrorAndDie.class);
    private final IReportError reportError;
    private final Runnable suicideFn;

    public ReportErrorAndDie(IReportError reportError, Runnable suicideFn) {
        this.reportError = reportError;
        this.suicideFn = suicideFn;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            reportError.report(e);
        } catch (Exception ex) {
            LOG.error("Error while reporting error to cluster, proceeding with shutdown", ex);
        }
        if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, e)
            || (Utils.exceptionCauseIsInstanceOf(java.io.InterruptedIOException.class, e)
                && !Utils.exceptionCauseIsInstanceOf(java.net.SocketTimeoutException.class, e))) {
            LOG.info("Got interrupted exception shutting thread down...");
        } else {
            suicideFn.run();
        }
    }
}
