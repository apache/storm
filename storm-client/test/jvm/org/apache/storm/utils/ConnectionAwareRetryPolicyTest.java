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
import java.util.function.Supplier;
import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.RetrySleeper;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.listen.Listenable;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionAwareRetryPolicy}. The ZooKeeper connection state is driven by
 * capturing the {@link ConnectionStateListener} that {@link ConnectionAwareRetryPolicy#bind} registers
 * and firing state transitions at it, so no real ZooKeeper is required.
 */
public class ConnectionAwareRetryPolicyTest {

    private static final int SESSION_TIMEOUT_MS = 20_000;

    private final RetryPolicy delegate = mock(RetryPolicy.class);
    private final RetrySleeper sleeper = mock(RetrySleeper.class);
    private final CuratorFramework zk = mock(CuratorFramework.class);
    private ConnectionStateListener listener;

    @SuppressWarnings("unchecked")
    private ConnectionAwareRetryPolicy build(Supplier<CuratorFramework> zkSupplier) {
        Listenable<ConnectionStateListener> listenable = mock(Listenable.class);
        when(zk.getConnectionStateListenable()).thenReturn(listenable);

        ConnectionAwareRetryPolicy policy = new ConnectionAwareRetryPolicy(delegate, zkSupplier, SESSION_TIMEOUT_MS);
        policy.bind(zk);

        ArgumentCaptor<ConnectionStateListener> captor = ArgumentCaptor.forClass(ConnectionStateListener.class);
        verify(listenable).addListener(captor.capture());
        listener = captor.getValue();
        return policy;
    }

    private void fire(ConnectionState state) {
        listener.stateChanged(zk, state);
    }

    @Test
    public void connectedStateDelegatesToBackoffPolicy() {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);

        // Default state is CONNECTED (no event fired).
        boolean result = policy.allowRetry(0, 0L, sleeper);

        assertTrue(result, "when connected, must propagate the delegate's decision");
        verify(delegate).allowRetry(0, 0L, sleeper);
    }

    @Test
    public void reconnectedStateDelegatesToBackoffPolicy() {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(false);

        fire(ConnectionState.RECONNECTED);
        boolean result = policy.allowRetry(2, 100L, sleeper);

        assertFalse(result, "RECONNECTED is a healthy state and must delegate");
        verify(delegate).allowRetry(2, 100L, sleeper);
    }

    @Test
    public void suspendedBlocksUntilConnectedThenRetriesImmediately() throws Exception {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);
        when(zk.blockUntilConnected(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenReturn(true);

        fire(ConnectionState.SUSPENDED);
        boolean result = policy.allowRetry(0, 0L, sleeper);

        assertTrue(result, "should retry once the SendThread has reconnected");
        verify(zk).blockUntilConnected(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void lostBlocksUntilConnectedThenRetriesImmediately() throws Exception {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);
        when(zk.blockUntilConnected(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenReturn(true);

        fire(ConnectionState.LOST);
        boolean result = policy.allowRetry(3, 1234L, sleeper);

        assertTrue(result, "LOST should also wait for reconnection then retry");
        verify(zk).blockUntilConnected(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void suspendedAbandonsRetryWhenReconnectTimesOut() throws Exception {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);
        when(zk.blockUntilConnected(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenReturn(false);

        fire(ConnectionState.SUSPENDED);
        boolean result = policy.allowRetry(0, 0L, sleeper);

        assertFalse(result, "should abandon when not reconnected within the session timeout");
    }

    @Test
    public void suspendedHonoursDelegateRetryBudget() throws Exception {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        // Delegate says no more retries allowed (budget exhausted)
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(false);

        fire(ConnectionState.SUSPENDED);
        boolean result = policy.allowRetry(100, 99999L, sleeper);

        assertFalse(result, "should abandon when the delegate's retry budget is exhausted");
        verify(zk, never()).blockUntilConnected(anyInt(), any());
    }

    @Test
    public void interruptedWhileWaitingReturnsFalseAndPreservesInterruptFlag() throws Exception {
        ConnectionAwareRetryPolicy policy = build(() -> zk);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);
        when(zk.blockUntilConnected(anyInt(), any())).thenThrow(new InterruptedException("test"));

        fire(ConnectionState.SUSPENDED);
        boolean result = policy.allowRetry(0, 0L, sleeper);

        assertFalse(result, "an interrupt while waiting should abandon the retry");
        assertTrue(Thread.interrupted(), "interrupt flag must be preserved (this check also clears it)");
    }

    @Test
    public void suspendedWithNullFrameworkFallsThroughToDelegate() {
        // zkSupplier returns null (framework not yet available), even though bind() ran on the mock.
        ConnectionAwareRetryPolicy policy = build(() -> null);
        when(delegate.allowRetry(anyInt(), anyLong(), any())).thenReturn(true);

        fire(ConnectionState.SUSPENDED);
        boolean result = policy.allowRetry(1, 50L, sleeper);

        assertTrue(result, "with no framework available yet, must fall through to the delegate");
        verify(delegate).allowRetry(1, 50L, sleeper);
    }
}
