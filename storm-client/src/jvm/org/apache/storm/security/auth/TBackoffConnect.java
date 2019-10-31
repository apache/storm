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

package org.apache.storm.security.auth;

import java.io.IOException;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TBackoffConnect {
    private static final Logger LOG = LoggerFactory.getLogger(TBackoffConnect.class);
    private int completedRetries = 0;
    private int retryTimes;
    private StormBoundedExponentialBackoffRetry waitGrabber;
    private boolean retryForever = false;

    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling, boolean retryForever) {

        this.retryForever = retryForever;
        this.retryTimes = retryTimes;
        waitGrabber = new StormBoundedExponentialBackoffRetry(retryInterval,
                                                              retryIntervalCeiling,
                                                              retryTimes);
    }

    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling) {
        this(retryTimes, retryInterval, retryIntervalCeiling, false);
    }

    public TTransport doConnectWithRetry(ITransportPlugin transportPlugin, TTransport underlyingTransport, String host,
                                         String asUser) throws IOException {
        boolean connected = false;
        TTransport transportResult = null;
        while (!connected) {
            try {
                transportResult = transportPlugin.connect(underlyingTransport, host, asUser);
                connected = true;
            } catch (TTransportException ex) {
                retryNext(ex);
            }
        }
        return transportResult;
    }

    private void retryNext(TTransportException ex) {
        if (!canRetry()) {
            throw new RuntimeException(ex);
        }
        try {
            long sleeptime = waitGrabber.getSleepTimeMs(completedRetries, 0);

            LOG.debug("Failed to connect. Retrying... (" + Integer.toString(completedRetries) + ") in " + Long.toString(sleeptime) + "ms");

            Thread.sleep(sleeptime);
        } catch (InterruptedException e) {
            LOG.info("Nimbus connection retry interrupted.");
        }

        completedRetries++;
    }

    private boolean canRetry() {
        return retryForever || (completedRetries < retryTimes);
    }
}
