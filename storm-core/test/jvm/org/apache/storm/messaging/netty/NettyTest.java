/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.messaging.netty;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.Testing;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.TransportFactory;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTest.class);
    
    private final AtomicBoolean[] remoteBpStatus = new AtomicBoolean[]{new AtomicBoolean(), new AtomicBoolean()};
    private final int taskId = 1;

    /**
     * In a "real" cluster (or an integration test), Storm itself would ensure that a topology's workers would only be activated once all
     * the workers' connections are ready. The tests in this file however launch Netty servers and clients directly, and thus we must ensure
     * manually that the server and the client connections are ready before we commence testing. If we don't do this, then we will lose the
     * first messages being sent between the client and the server, which will fail the tests.
     */
    private void waitUntilReady(IConnection... connections) throws Exception {
        LOG.info("Waiting until all Netty connections are ready...");
        int intervalMs = 10;
        int maxWaitMs = 5000;
        int waitedMs = 0;
        while (true) {
            if (Arrays.asList(connections).stream()
                .allMatch(WorkerState::isConnectionReady)) {
                LOG.info("All Netty connections are ready");
                break;
            }
            if (waitedMs > maxWaitMs) {
                throw new RuntimeException("Netty connections were not ready within " + maxWaitMs + " ms");
            }
            Thread.sleep(intervalMs);
            waitedMs += intervalMs;
        }
    }

    private IConnectionCallback mkConnectionCallback(Consumer<TaskMessage> myFn) {
        return (batch) -> {
            batch.forEach(myFn::accept);
        };
    }

    private Runnable sleep() {
        return () -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw Utils.wrapInRuntime(e);
            }
        };
    }

    private void waitForNotNull(AtomicReference<TaskMessage> response) {
        Testing.whileTimeout(Testing.TEST_TIMEOUT_MS,
            () -> response.get() == null,
            sleep());
    }

    private void send(IConnection client, int taskId, byte[] messageBytes) {
        client.send(Collections.singleton(new TaskMessage(taskId, messageBytes)).iterator());
    }

    private void doTestBasic(Map<String, Object> stormConf) throws Exception {
        LOG.info("1. Should send and receive a basic message");
        String reqMessage = "0123456789abcdefghijklmnopqrstuvwxyz";
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            try (IConnection server = context.bind(null, 0, mkConnectionCallback(response::set), null);
                IConnection client = context.connect(null, "localhost", server.getPort(), remoteBpStatus)) {
                waitUntilReady(client, server);
                byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);

                send(client, taskId, messageBytes);

                waitForNotNull(response);
                TaskMessage responseMessage = response.get();
                assertThat(responseMessage.task(), is(taskId));
                assertThat(responseMessage.message(), is(messageBytes));
            }
        } finally {
            context.term();
        }
    }

    private Map<String, Object> basicConf() {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging.netty.Context");
        stormConf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        stormConf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
        stormConf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
        stormConf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
        stormConf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
        stormConf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
        stormConf.put(Config.STORM_MESSAGING_NETTY_BUFFER_LOW_WATERMARK, 8388608);
        stormConf.put(Config.STORM_MESSAGING_NETTY_BUFFER_HIGH_WATERMARK, 16777216);
        stormConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT, 1);
        stormConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT, 1000);
        stormConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS, 1);
        stormConf.put(Config.TOPOLOGY_KRYO_FACTORY, "org.apache.storm.serialization.DefaultKryoFactory");
        stormConf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "org.apache.storm.serialization.types.ListDelegateSerializer");
        stormConf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
        stormConf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, false);
        return stormConf;
    }

    private Map<String, Object> withSaslConf(Map<String, Object> stormConf) {
        stormConf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, true);
        stormConf.put(Config.TOPOLOGY_NAME, "topo1-netty-sasl");
        stormConf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, Utils.secureRandomLong() + ":" + Utils.secureRandomLong());
        return stormConf;
    }

    @Test
    public void testBasic() throws Exception {
        doTestBasic(basicConf());
    }

    @Test
    public void testBasicWithSasl() throws Exception {
        doTestBasic(withSaslConf(basicConf()));
    }
    
    private void doTestLoad(Map<String, Object> stormConf) throws Exception {
        LOG.info("2 test load");
        String reqMessage = "0123456789abcdefghijklmnopqrstuvwxyz";
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            try (IConnection server = context.bind(null, 0, mkConnectionCallback(response::set), null);
                IConnection client = context.connect(null, "localhost", server.getPort(), remoteBpStatus)) {
                waitUntilReady(client, server);
                byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);

                send(client, taskId, messageBytes);
                /*
                 * This test sends a broadcast to all connected clients from the server, so we need to wait until the server has registered
                 * the client as connected before sending load metrics.
                 *
                 * It's not enough to wait until the client reports that the channel is open, because the server event loop may not have
                 * finished running channelActive for the new channel. If we send metrics too early, the server will broadcast to no one.
                 *
                 * By waiting for the response here, we ensure that the client will be registered at the server before we send load metrics.
                 */

                waitForNotNull(response);

                Map<Integer, Double> taskToLoad = new HashMap<>();
                taskToLoad.put(1, 0.0);
                taskToLoad.put(2, 1.0);
                server.sendLoadMetrics(taskToLoad);

                List<Integer> tasks = new ArrayList<>();
                tasks.add(1);
                tasks.add(2);
                Testing.whileTimeout(Testing.TEST_TIMEOUT_MS,
                    () -> client.getLoad(tasks).isEmpty(),
                    sleep());
                Map<Integer, Load> load = client.getLoad(tasks);
                assertThat(load.get(1).getBoltLoad(), is(0.0));
                assertThat(load.get(2).getBoltLoad(), is(1.0));
            }
        } finally {
            context.term();
        }
    }

    @Test
    public void testLoad() throws Exception {
        doTestLoad(basicConf());
    }

    @Test
    public void testLoadWithSasl() throws Exception {
        doTestLoad(withSaslConf(basicConf()));
    }

    private void doTestLargeMessage(Map<String, Object> stormConf) throws Exception {
        LOG.info("3 Should send and receive a large message");
        String reqMessage = StringUtils.repeat("c", 2_048_000);
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            try (IConnection server = context.bind(null, 0, mkConnectionCallback(response::set), null);
                IConnection client = context.connect(null, "localhost", server.getPort(), remoteBpStatus)) {
                waitUntilReady(client, server);
                byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);

                send(client, taskId, messageBytes);

                waitForNotNull(response);
                TaskMessage responseMessage = response.get();
                assertThat(responseMessage.task(), is(taskId));
                assertThat(responseMessage.message(), is(messageBytes));
            }
        } finally {
            context.term();
        }
    }

    private Map<String, Object> largeMessageConf() {
        Map<String, Object> conf = basicConf();
        conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 102_400);
        return conf;
    }

    @Test
    public void testLargeMessage() throws Exception {
        doTestLargeMessage(largeMessageConf());
    }

    @Test
    public void testLargeMessageWithSasl() throws Exception {
        doTestLargeMessage(withSaslConf(largeMessageConf()));
    }

    private void doTestServerDelayed(Map<String, Object> stormConf) throws Exception {
        LOG.info("4. test server delayed");
        String reqMessage = "0123456789abcdefghijklmnopqrstuvwxyz";
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            int port = Utils.getAvailablePort(6700);
            try (IConnection client = context.connect(null, "localhost", port, remoteBpStatus)) {
                AtomicReference<IConnection> server = new AtomicReference<>();
                try {
                    CompletableFuture<?> serverStart = CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(100);
                            server.set(context.bind(null, port, mkConnectionCallback(response::set), null));
                            waitUntilReady(client, server.get());
                        } catch (Exception e) {
                            throw Utils.wrapInRuntime(e);
                        }
                    });
                    serverStart.get(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);

                    send(client, taskId, messageBytes);

                    waitForNotNull(response);
                    TaskMessage responseMessage = response.get();
                    assertThat(responseMessage.task(), is(taskId));
                    assertThat(responseMessage.message(), is(messageBytes));
                } finally {
                    if (server.get() != null) {
                        server.get().close();
                    }
                }
            }
        } finally {
            context.term();
        }
    }

    @Test
    public void testServerDelayed() throws Exception {
        doTestServerDelayed(basicConf());
    }

    @Test
    public void testServerDelayedWithSasl() throws Exception {
        doTestServerDelayed(withSaslConf(basicConf()));
    }

    private void doTestBatch(Map<String, Object> stormConf) throws Exception {
        int numMessages = 100_000;
        LOG.info("Should send and receive many messages (testing with " + numMessages + " messages)");
        ArrayList<TaskMessage> responses = new ArrayList<>();
        AtomicInteger received = new AtomicInteger();
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            try (IConnection server = context.bind(null, 0, mkConnectionCallback((message) -> {
                    responses.add(message);
                    received.incrementAndGet();
                }), null);
                IConnection client = context.connect(null, "localhost", server.getPort(), remoteBpStatus)) {
                waitUntilReady(client, server);

                IntStream.range(1, numMessages)
                    .forEach(i -> send(client, taskId, String.valueOf(i).getBytes(StandardCharsets.UTF_8)));

                Testing.whileTimeout(Testing.TEST_TIMEOUT_MS,
                    () -> responses.size() < numMessages - 1,
                    () -> {
                        LOG.info("{} of {} received", responses.size(), numMessages - 1);
                        sleep().run();
                    });
                IntStream.range(1, numMessages)
                    .forEach(i -> {
                        assertThat(new String(responses.get(i - 1).message(), StandardCharsets.UTF_8), is(String.valueOf(i)));
                    });
            }
        } finally {
            context.term();
        }
    }

    private Map<String, Object> batchConf() {
        Map<String, Object> conf = basicConf();
        conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1_024_000);
        return conf;
    }

    @Test
    public void testBatch() throws Exception {
        doTestBatch(batchConf());
    }

    @Test
    public void testBatchWithSasl() throws Exception {
        doTestBatch(withSaslConf(batchConf()));
    }

    private void doTestServerAlwaysReconnects(Map<String, Object> stormConf) throws Exception {
        LOG.info("6. test server always reconnects");
        String reqMessage = "0123456789abcdefghijklmnopqrstuvwxyz";
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            int port = Utils.getAvailablePort(6700);
            try (IConnection client = context.connect(null, "localhost", port, remoteBpStatus)) {
                byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);
                send(client, taskId, messageBytes);
                try (IConnection server = context.bind(null, port, mkConnectionCallback(response::set), null)) {
                    waitUntilReady(client, server);
                    send(client, taskId, messageBytes);
                    waitForNotNull(response);
                    TaskMessage responseMessage = response.get();
                    assertThat(responseMessage.task(), is(taskId));
                    assertThat(responseMessage.message(), is(messageBytes));
                }
            }
        } finally {
            context.term();
        }
    }

    @Test
    public void testServerAlwaysReconnects() throws Exception {
        doTestServerAlwaysReconnects(basicConf());
    }

    @Test
    public void testServerAlwaysReconnectsWithSasl() throws Exception {
        doTestServerAlwaysReconnects(withSaslConf(basicConf()));
    }

    private void connectToFixedPort(Map<String, Object> stormConf, int port) throws Exception {
        LOG.info("7. Should be able to rebind to a port quickly");
        String reqMessage = "0123456789abcdefghijklmnopqrstuvwxyz";
        IContext context = TransportFactory.makeContext(stormConf, null);
        try {
            AtomicReference<TaskMessage> response = new AtomicReference<>();
            try (IConnection server = context.bind(null, port, mkConnectionCallback(response::set), null);
                 IConnection client = context.connect(null, "localhost", server.getPort(), remoteBpStatus)) {
                waitUntilReady(client, server);
                byte[] messageBytes = reqMessage.getBytes(StandardCharsets.UTF_8);

                send(client, taskId, messageBytes);

                waitForNotNull(response);
                TaskMessage responseMessage = response.get();
                assertThat(responseMessage.task(), is(taskId));
                assertThat(responseMessage.message(), is(messageBytes));
            }
        } finally {
            context.term();
        }
    }

    @Test
    public void testRebind() throws Exception {
        for (int i = 0; i < 10; ++i) {
            final long startTime = System.nanoTime();
            LOG.info("Binding to port 6700 iter: " + (i+1));
            connectToFixedPort(basicConf(), 6700);
            final long endTime = System.nanoTime();
            LOG.info("Expected time taken should be less than 5 sec, actual time is: " + (endTime-startTime)/1_000_000 + " ms");
            assertThat((endTime-startTime)/1_000_000, lessThan(5_000L));
        }
    }

}
