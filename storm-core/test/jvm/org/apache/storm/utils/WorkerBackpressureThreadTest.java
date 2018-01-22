/**
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

package org.apache.storm.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerBackpressureThreadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerBackpressureThreadTest.class);

    @Test
    public void testNormalEvent() throws Exception {
        Object trigger = new Object();
        CountDownLatch latch = new CountDownLatch(1);
        WorkerBackpressureCallback callback = new WorkerBackpressureCallback() {
            @Override
            public void onEvent(Object obj) {
                ((CountDownLatch) obj).countDown();
            }
        };
        WorkerBackpressureThread workerBackpressureThread = new WorkerBackpressureThread(trigger, latch, callback);
        workerBackpressureThread.start();
        WorkerBackpressureThread.notifyBackpressureChecker(trigger);
        //The callback should be called when the trigger is notified
        assertThat(latch.await(1, TimeUnit.SECONDS), is(true));
    }
}
