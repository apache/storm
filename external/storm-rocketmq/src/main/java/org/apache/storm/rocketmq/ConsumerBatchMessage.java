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

package org.apache.storm.rocketmq;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerBatchMessage<T> {
    private final List<T> data;
    private CountDownLatch latch;
    private boolean hasFailure = false;

    public ConsumerBatchMessage(List<T> data) {
        this.data = data;
        latch = new CountDownLatch(data.size());
    }

    public boolean waitFinish(long timeout) throws InterruptedException {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }

    public boolean isSuccess() {
        return !hasFailure;
    }

    public List<T> getData() {
        return data;
    }

    /**
     * Countdown if the sub message is successful.
     */
    public void ack() {
        latch.countDown();
    }

    /**
     * Countdown and fail-fast if the sub message is failed.
     */
    public void fail() {
        hasFailure = true;
        // fail fast
        long count = latch.getCount();
        for (int i = 0; i < count; i++) {
            latch.countDown();
        }
    }
}
