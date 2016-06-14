/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout.utils;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does an infinite constant time backoff against an exception.
 *
 * @param <T> return type
 */
public class InfiniteConstantBackoffRetry<T> implements Callable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(InfiniteConstantBackoffRetry.class);

    private final long backoffMillis;
    private final Class<? extends Exception> retryOn;
    private final Callable<T> f;

    /** Constructor.
     * @param backoffMillis Backoff time
     * @param retryOn Exception we should retry on.
     * @param f Callable (function) we should call/retry
     */
    public InfiniteConstantBackoffRetry(final long backoffMillis,
            final Class<? extends Exception> retryOn,
            final Callable<T> f) {
        this.backoffMillis = backoffMillis;
        this.retryOn = retryOn;
        this.f = f;
    }

    @Override
    public T call() {
        try {
            return checkedCall();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T checkedCall() throws Exception {
        while (true) {
            try {
                return f.call();
            } catch (Exception e) {
                if (retryOn.isAssignableFrom(e.getClass())) {
                    LOG.debug("Caught exception of type " + retryOn.getName() + ", backing off for " + backoffMillis
                            + " ms.");
                    Thread.sleep(backoffMillis);
                } else {
                    throw e;
                }
            }
        }
    }
}
