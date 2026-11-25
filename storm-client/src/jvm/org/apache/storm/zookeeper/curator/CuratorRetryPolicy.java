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

package org.apache.storm.zookeeper.curator;

import org.apache.storm.shade.org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;

/**
 * Internal. Adapts {@link StormBoundedExponentialBackoffRetry} to Curator's retry policy.
 *
 * <p>Extending {@link BoundedExponentialBackoffRetry} with the same arguments keeps Curator's own
 * handling (the maximum-retries cap and {@code allowRetry} semantics), while the sleep time is
 * computed by Storm's backoff algorithm.
 */
final class CuratorRetryPolicy extends BoundedExponentialBackoffRetry {
    private final StormBoundedExponentialBackoffRetry delegate;

    CuratorRetryPolicy(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        this.delegate = new StormBoundedExponentialBackoffRetry(baseSleepTimeMs, maxSleepTimeMs,
                maxRetries);
    }

    @Override
    public long getSleepTimeMs(int retryCount, long elapsedTimeMs) {
        return delegate.getSleepTimeMs(retryCount, elapsedTimeMs);
    }
}
