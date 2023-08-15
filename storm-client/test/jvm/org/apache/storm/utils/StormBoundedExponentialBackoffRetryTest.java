/*
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

package org.apache.storm.utils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StormBoundedExponentialBackoffRetryTest {
    private static final Logger LOG = LoggerFactory.getLogger(StormBoundedExponentialBackoffRetryTest.class);

    @Test
    public void testExponentialSleepLargeRetries() {
        int baseSleepMs = 10;
        int maxSleepMs = 1000;
        int maxRetries = 900;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);

    }

    @Test
    public void testExponentialSleep() {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 40;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepSmallMaxRetries() {
        int baseSleepMs = 1000;
        int maxSleepMs = 5000;
        int maxRetries = 10;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepZeroMaxTries() {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 0;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepSmallMaxTries() {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 10;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    private void validateSleepTimes(int baseSleepMs, int maxSleepMs, int maxRetries) {
        StormBoundedExponentialBackoffRetry retryPolicy = new StormBoundedExponentialBackoffRetry(baseSleepMs, maxSleepMs, maxRetries);
        int retryCount = 0;
        long prevSleepMs = 0;
        LOG.info("The baseSleepMs [" + baseSleepMs + "] the maxSleepMs [" + maxSleepMs +
                 "] the maxRetries [" + maxRetries + "]");
        while (retryCount <= maxRetries) {
            long currSleepMs = retryPolicy.getSleepTimeMs(retryCount, 0);
            LOG.info("For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs +
                     "] the currentSleepMs [" + currSleepMs + "]");
            assertTrue((prevSleepMs < currSleepMs) || (currSleepMs == maxSleepMs),
                "For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs +
                "] is not less than currentSleepMs [" + currSleepMs + "]");
            assertTrue((baseSleepMs <= currSleepMs) || (currSleepMs == maxSleepMs),
                "For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs +
                    "] is less than baseSleepMs [" + baseSleepMs + "].");
            assertTrue(maxSleepMs >= currSleepMs,
                "For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs +
                    "] is greater than maxSleepMs [" + maxSleepMs + "]");
            prevSleepMs = currSleepMs;
            retryCount++;
        }
        int badRetryCount = maxRetries + 10;
        long currSleepMs = retryPolicy.getSleepTimeMs(badRetryCount, 0);
        LOG.info("For badRetryCount [" + badRetryCount + "] the previousSleepMs [" + prevSleepMs +
                 "] the currentSleepMs [" + currSleepMs + "]");
        assertTrue(maxSleepMs >= currSleepMs,
            "For the badRetryCount [" + badRetryCount + "] that's greater than maxRetries [" +
                maxRetries + "]the currentSleepMs [" + currSleepMs + "] " +
                "is greater than maxSleepMs [" + maxSleepMs + "]");
    }
}

