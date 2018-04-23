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

package org.apache.storm.messaging.netty;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NettyUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            LOG.error("Uncaught exception in netty " + e.getCause());
        } catch (Throwable err) {
            // Doing nothing (probably due to an oom issue) and hoping Utils.handleUncaughtException will handle it
        }

        try {
            Utils.handleUncaughtException(e);
        } catch (Throwable throwable) {
            LOG.error("Exception thrown while handling uncaught exception " + throwable.getCause());
        }
        LOG.info("Received error in netty thread.. terminating server...");
        Runtime.getRuntime().exit(1);
    }
}
