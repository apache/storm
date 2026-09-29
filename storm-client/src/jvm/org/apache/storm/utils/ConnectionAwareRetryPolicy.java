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

package org.apache.storm.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.RetrySleeper;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RetryPolicy} wrapper that makes the Curator retry loop aware of the
 * ZooKeeper connection state.
 *
 * <p>When the connection is {@link ConnectionState#SUSPENDED} or
 * {@link ConnectionState#LOST}, instead of blindly sleeping and retrying (which
 * races against the ZK client's {@code SendThread} reconnection), this policy
 * calls {@link CuratorFramework#blockUntilConnected} to yield to the
 * {@code SendThread} and wait for it to failover to another ensemble member.
 *
 * <p>Once the connection is re-established, the retry loop immediately retries
 * the operation on the new connection. If the connection cannot be
 * re-established within the session timeout, the retry is abandoned.
 *
 * <p>For all other connection states, this policy delegates to the wrapped
 * delegate policy (typically a {@link StormBoundedExponentialBackoffRetry}).
 */
public class ConnectionAwareRetryPolicy implements RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionAwareRetryPolicy.class);

    private final RetryPolicy delegate;
    private final Supplier<CuratorFramework> zkSupplier;
    private final int sessionTimeoutMs;
    private final AtomicReference<ConnectionState> connectionState =
        new AtomicReference<>(ConnectionState.CONNECTED);

    /**
     * @param delegate         the underlying retry policy to delegate to for normal (connected) retries
     * @param zkSupplier       supplier for the {@link CuratorFramework}, used for late binding since
     *                         the framework does not exist until {@code builder.build()} returns
     * @param sessionTimeoutMs upper bound (in ms) for waiting on reconnection; typically
     *                         {@code storm.zookeeper.session.timeout}
     */
    public ConnectionAwareRetryPolicy(RetryPolicy delegate,
                                      Supplier<CuratorFramework> zkSupplier,
                                      int sessionTimeoutMs) {
        this.delegate = delegate;
        this.zkSupplier = zkSupplier;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /**
     * Register a {@link org.apache.storm.shade.org.apache.curator.framework.state.ConnectionStateListener}
     * on the given framework to track the current connection state.
     * Must be called after the {@link CuratorFramework} has been built.
     *
     * @param zk the built CuratorFramework
     */
    public void bind(CuratorFramework zk) {
        zk.getConnectionStateListenable().addListener((client, newState) -> {
            ConnectionState prev = connectionState.getAndSet(newState);
            if (prev != newState) {
                LOG.debug("ZK connection state changed: {} -> {}", prev, newState);
            }
        });
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleepSleeper) {
        ConnectionState state = connectionState.get();

        if (state == ConnectionState.SUSPENDED || state == ConnectionState.LOST) {
            CuratorFramework zk = zkSupplier.get();
            if (zk == null) {
                // Framework not yet available; fall through to delegate
                return delegate.allowRetry(retryCount, elapsedTimeMs, sleepSleeper);
            }

            LOG.info("ZK connection is {} on retry {}, waiting for reconnection (timeout {}ms)",
                state, retryCount, sessionTimeoutMs);
            try {
                boolean reconnected = zk.blockUntilConnected(sessionTimeoutMs, TimeUnit.MILLISECONDS);
                if (reconnected) {
                    LOG.info("ZK connection re-established (state: {}), retrying operation",
                        connectionState.get());
                    // Return true without sleeping — the SendThread has already
                    // failedover to another ensemble member. Retry immediately.
                    return true;
                }
                LOG.warn("ZK connection not re-established within {}ms, abandoning retry",
                    sessionTimeoutMs);
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for ZK reconnection", e);
                return false;
            }
        }

        // Connection is healthy (CONNECTED, RECONNECTED, or READ_ONLY) —
        // delegate to the existing backoff policy for normal retry behaviour.
        return delegate.allowRetry(retryCount, elapsedTimeMs, sleepSleeper);
    }
}
