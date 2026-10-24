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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.api.CuratorEventType;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link ConnectionAwareRetryPolicy} against a real ZooKeeper server that is stopped
 * and restarted after a short outage.
 *
 * <p>Curator also consults the retry policy from the ZooKeeper EventThread, when it retries a
 * background operation inside its async callback
 * ({@code CuratorFrameworkImpl.checkBackgroundRetry}). That thread is the one that must deliver
 * {@code SyncConnected}. If the policy blocks it in {@code blockUntilConnected}, Curator never
 * sees {@code RECONNECTED}, and once the session timeout elapses while {@code SUSPENDED} it
 * injects a session expiration, so a session that had already recovered is lost.
 */
public class ConnectionAwareRetryPolicyEventThreadTest {

    private static final int SESSION_TIMEOUT_MS = 6000;
    private static final int CONNECTION_TIMEOUT_MS = 3000;
    private static final long OUTAGE_MS = 500;

    /**
     * Control case: off the EventThread the policy waits for the reconnection and allows the retry.
     */
    @Test
    public void allowRetryOffEventThreadSurvivesShortOutage() throws Exception {
        Outcome outcome = run(false);
        assertTrue(outcome.allowed, "retry should be allowed once the server is back: " + outcome);
        assertFalse(outcome.lost(), "a short outage must not lose the session: " + outcome);
    }

    /**
     * Fails with the current implementation: the policy blocks the EventThread for the whole
     * session timeout, returns false, and Curator injects a session expiration (LOST).
     */
    @Test
    public void allowRetryOnEventThreadSurvivesShortOutage() throws Exception {
        Outcome outcome = run(true);
        assertTrue(outcome.allowed, "retry should be allowed once the server is back: " + outcome);
        assertFalse(outcome.lost(), "a short outage must not lose the session: " + outcome);
    }

    private Outcome run(boolean onEventThread) throws Exception {
        try (TestingServer server = new TestingServer(true)) {
            Map<String, Object> conf = Utils.readDefaultConfig();
            conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, SESSION_TIMEOUT_MS);
            conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_MS);
            String[] hostPort = server.getConnectString().split(":");
            CuratorFramework zk = CuratorUtils.newCuratorStarted(conf, List.of(hostPort[0]),
                Integer.parseInt(hostPort[1]), null, null);
            try {
                assertTrue(zk.blockUntilConnected(10, TimeUnit.SECONDS), "initial connection");
                RetryPolicy policy = zk.getZookeeperClient().getRetryPolicy();
                AtomicReference<ConnectionState> policyState = policyState(policy);

                List<ConnectionState> states = new CopyOnWriteArrayList<>();
                zk.getConnectionStateListenable().addListener((client, state) -> states.add(state));

                CountDownLatch disconnected = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(1);
                Outcome outcome = new Outcome(states);
                Runnable callPolicy = () -> {
                    try {
                        // Same condition the policy checks: wait until its listener saw SUSPENDED.
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                        while (policyState.get() != ConnectionState.SUSPENDED
                            && System.nanoTime() < deadline) {
                            Thread.sleep(5);
                        }
                        outcome.thread = Thread.currentThread().getName();
                        long start = System.nanoTime();
                        outcome.allowed = policy.allowRetry(0, 0, (time, unit) -> { });
                        outcome.blockedMs = (System.nanoTime() - start) / 1_000_000;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                };
                // CuratorListener callbacks for connection events run on the ZooKeeper EventThread,
                // the same thread that runs checkBackgroundRetry.
                zk.getCuratorListenable().addListener((client, event) -> {
                    Watcher.Event.KeeperState state = event.getType() == CuratorEventType.WATCHED
                        ? event.getWatchedEvent().getState() : null;
                    if (state == Watcher.Event.KeeperState.Disconnected
                        && disconnected.getCount() > 0) {
                        disconnected.countDown();
                        if (onEventThread) {
                            callPolicy.run();
                        } else {
                            new Thread(callPolicy, "policy-caller").start();
                        }
                    }
                });

                server.stop();
                assertTrue(disconnected.await(10, TimeUnit.SECONDS), "client saw the disconnect");
                Thread.sleep(OUTAGE_MS);
                server.restart();
                assertTrue(done.await(30, TimeUnit.SECONDS), "policy call completed");
                // Give Curator time to process the events queued behind a blocked EventThread.
                Thread.sleep(1000);
                assertNotNull(outcome.thread);
                return outcome;
            } finally {
                zk.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<ConnectionState> policyState(RetryPolicy policy)
        throws ReflectiveOperationException {
        Field field = ConnectionAwareRetryPolicy.class.getDeclaredField("connectionState");
        field.setAccessible(true);
        return (AtomicReference<ConnectionState>) field.get(policy);
    }

    private static final class Outcome {
        private final List<ConnectionState> states;
        private volatile String thread;
        private volatile boolean allowed;
        private volatile long blockedMs;

        Outcome(List<ConnectionState> states) {
            this.states = states;
        }

        boolean lost() {
            return states.contains(ConnectionState.LOST);
        }

        @Override
        public String toString() {
            return "thread=" + thread + " allowRetry=" + allowed + " blockedMs=" + blockedMs
                + " states=" + states;
        }
    }
}
