/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.security.auth.SimpleTransportPlugin;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.security.auth.digest.DigestSaslTransportPlugin;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * Benchmark for {@code storm.virtual.threads.enabled} on the Nimbus Thrift handler pool.
 *
 * <p>Starts an in-process {@link ThriftServer} whose {@code getNimbusConf} handler sleeps for a configurable time
 * to simulate blocking I/O (ZooKeeper, blob store), then hammers it with N client threads and reports throughput,
 * latency percentiles, peak platform-thread count, an estimate of the server-side platform threads and RSS. Run it
 * with the flag off and on to compare. The server can use either {@link SimpleTransportPlugin}
 * ({@code THsHaServer}) or {@link DigestSaslTransportPlugin} ({@code TThreadPoolServer}, one handler thread per
 * connection, as used by the SASL and TLS transports of secure clusters); the digest mode writes a throwaway JAAS
 * file with a fixed user and needs {@code --threads >= --clients}, since every client holds its connection, and
 * with it a handler thread, for the whole run.
 *
 * <p>Build and run (JDK 25, Maven 3.9):
 * <pre>
 * mvn -pl storm-client,examples/storm-perf -am install -DskipTests -Dcheckstyle.skip=true -q
 * mvn -pl examples/storm-perf dependency:build-classpath -Dmdep.outputFile=target/cp.txt -q
 * java -cp examples/storm-perf/target/classes:$(cat examples/storm-perf/target/cp.txt) \
 *     org.apache.storm.perf.ThriftHandlerVirtualThreadsBench --mode off --clients 200 --calls 50 --io-ms 10
 *     java -cp examples/storm-perf/target/classes:$(cat examples/storm-perf/target/cp.txt) \
 *     org.apache.storm.perf.ThriftHandlerVirtualThreadsBench --mode on --clients 200 --calls 50 --io-ms 10
 * </pre>
 *
 * <p>Options: {@code --mode off|on|both} (default {@code off}; {@code both} runs the two modes in one JVM, which
 * shares heap and JIT state between them and is only meant for a quick look), {@code --clients N}, {@code --calls M}
 * (per client), {@code --transport simple|digest} (default {@code simple}),
 * {@code --io-ms L} (simulated handler I/O), {@code --threads T} ({@code nimbus.thrift.threads}),
 * {@code --queue-size Q|none} ({@code nimbus.queue.size}; defaults to the shipped 100000, so both modes use a pool of
 * {@code --threads} handlers; {@code none} removes the key so the platform-thread run falls back to THsHaServer's own
 * pool, whose effective concurrency is 5), {@code --warmup W} (calls per client excluded from statistics). The report
 * and its JSON line carry the queue size the server actually ran with ({@code null} only for {@code none}).
 *
 * <p>For comparable numbers run each mode in its own JVM ({@code --mode off} then {@code --mode on}).
 */
public final class ThriftHandlerVirtualThreadsBench {

    private static final String HOST = "localhost";
    private static final int CONCURRENT_CONNECTS = 16;

    private ThriftHandlerVirtualThreadsBench() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String mode = opts.getOrDefault("mode", "off").toLowerCase(Locale.ROOT);
        if (!mode.equals("off") && !mode.equals("on") && !mode.equals("both")) {
            throw new IllegalArgumentException("Unknown --mode " + mode + ", expected off, on or both");
        }
        int clients = Integer.parseInt(opts.getOrDefault("clients", "200"));
        int calls = Integer.parseInt(opts.getOrDefault("calls", "50"));
        long ioMs = Long.parseLong(opts.getOrDefault("io-ms", "10"));
        int threads = Integer.parseInt(opts.getOrDefault("threads", "64"));
        String queueOpt = opts.get("queue-size");
        boolean noQueue = "none".equalsIgnoreCase(queueOpt);
        Integer queueSize = queueOpt == null || noQueue ? null : Integer.valueOf(queueOpt);
        int warmup = Integer.parseInt(opts.getOrDefault("warmup", "5"));
        String transport = opts.getOrDefault("transport", "simple").toLowerCase(Locale.ROOT);
        if (!transport.equals("simple") && !transport.equals("digest")) {
            throw new IllegalArgumentException("Unknown --transport " + transport + ", expected simple or digest");
        }
        if (transport.equals("digest") && clients > threads) {
            // TThreadPoolServer dedicates a handler thread to each connection for its lifetime, and every client
            // keeps its connection open for the whole run, so connections beyond --threads would sit in the
            // executor queue until their SASL handshake times out.
            throw new IllegalArgumentException("--transport digest needs --threads >= --clients (one handler thread per"
                                               + " open connection); got threads=" + threads + " clients=" + clients);
        }

        if (ioMs < 0) {
            throw new IllegalArgumentException("--io-ms must be >= 0, got " + ioMs);
        }
        System.out.printf(Locale.ROOT, "transport=%s clients=%d calls=%d io-ms=%d threads=%d queue-size=%s warmup=%d%n",
                          transport, clients, calls, ioMs, threads, queueSizeLabel(queueSize, noQueue), warmup);
        if (mode.equals("off") || mode.equals("both")) {
            runPhase(false, transport, clients, calls, ioMs, threads, queueSize, noQueue, warmup);
        }
        if (mode.equals("on") || mode.equals("both")) {
            runPhase(true, transport, clients, calls, ioMs, threads, queueSize, noQueue, warmup);
        }
    }

    private static void runPhase(boolean virtual, String transport, int clients, int calls, long ioMs, int threads,
                                 Integer queueSize, boolean noQueue, int warmup) throws Exception {
        Map<String, Object> conf = new HashMap<>(Utils.readDefaultConfig());
        if (transport.equals("digest")) {
            conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, DigestSaslTransportPlugin.class.getName());
            conf.put("java.security.auth.login.config", writeDigestJaasConf().toString());
        } else {
            conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, SimpleTransportPlugin.class.getName());
        }
        conf.put(Config.NIMBUS_THRIFT_PORT, 0);
        conf.put(Config.NIMBUS_THRIFT_THREADS, threads);
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, virtual);
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
        if (noQueue) {
            conf.remove(Config.NIMBUS_QUEUE_SIZE);
        } else if (queueSize != null) {
            conf.put(Config.NIMBUS_QUEUE_SIZE, queueSize);
        }
        // What the server actually runs with: the explicit value, the shipped default, or none.
        Integer effectiveQueueSize = ThriftConnectionType.NIMBUS.getQueueSize(conf);

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peakInFlight = new AtomicInteger();
        Nimbus.Iface handler = sleepingHandler(ioMs, inFlight, peakInFlight);
        ThriftServer server = new ThriftServer(conf, new Nimbus.Processor<>(handler), ThriftConnectionType.NIMBUS);
        // Note: a transport failure inside serve() is handled by ThriftServer itself, which logs it and halts the
        // JVM with exit code 1 (ThriftServer.handleServerException), so it never reaches serveFailure; only an
        // Error thrown by serve() does. A silent exit code 1 before any result line means the server failed to
        // start (bad JAAS file, unbindable port, ...).
        AtomicReference<Throwable> serveFailure = new AtomicReference<>();
        Thread serveThread = new Thread(() -> {
            try {
                server.serve();
            } catch (Throwable t) {
                serveFailure.set(t);
            }
        }, "bench-thrift-serve");
        serveThread.setDaemon(true);
        serveThread.start();
        long serveDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!server.isServing()) {
            if (serveFailure.get() != null) {
                throw new IllegalStateException("Thrift server failed to start", serveFailure.get());
            }
            if (!serveThread.isAlive()) {
                throw new IllegalStateException("Thrift server thread exited before serving");
            }
            if (System.nanoTime() > serveDeadline) {
                throw new IllegalStateException("Thrift server did not start serving within 30s");
            }
            Thread.sleep(10);
        }
        int port = server.getPort();

        ThreadMXBean threadMx = ManagementFactory.getThreadMXBean();
        long rssBeforeKb = rssKb();
        int threadsBefore = threadMx.getThreadCount();
        AtomicInteger peakThreads = new AtomicInteger(threadsBefore);
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                peakThreads.accumulateAndGet(threadMx.getThreadCount(), Math::max);
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "bench-thread-sampler");
        sampler.setDaemon(true);
        sampler.start();

        long[][] latenciesNs = new long[clients][calls];
        List<Throwable> failures = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(clients);
        // The server socket uses the default listen backlog, so with hundreds of clients opening at once part of
        // the connections would be reset before the run starts. Connection setup is not what is measured, so
        // limit how many clients connect concurrently; calls are only issued once every client is connected.
        Semaphore connecting = new Semaphore(CONCURRENT_CONNECTS);
        for (int c = 0; c < clients; c++) {
            final int clientIdx = c;
            Thread t = new Thread(() -> {
                try (NimbusClient client = connect(conf, port, connecting)) {
                    Nimbus.Iface iface = client.getClient();
                    for (int i = 0; i < warmup; i++) {
                        iface.getNimbusConf();
                    }
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < calls; i++) {
                        long start = System.nanoTime();
                        iface.getNimbusConf();
                        latenciesNs[clientIdx][i] = System.nanoTime() - start;
                    }
                } catch (Throwable e) {
                    synchronized (failures) {
                        failures.add(e);
                    }
                    ready.countDown();
                } finally {
                    done.countDown();
                }
            }, "bench-client-" + c);
            t.start();
        }
        ready.await();
        long startNs = System.nanoTime();
        go.countDown();
        done.await();
        long elapsedNs = System.nanoTime() - startNs;

        sampler.interrupt();
        sampler.join(1000);
        long rssAfterKb = rssKb();
        server.stop();
        serveThread.join(5000);
        if (!failures.isEmpty()) {
            System.err.println("First client failure (" + failures.size() + " in total):");
            failures.get(0).printStackTrace();
        }

        report(virtual, transport, clients, calls, ioMs, threads, effectiveQueueSize, latenciesNs, elapsedNs,
               failures.size(), threadsBefore, peakThreads.get(), peakInFlight.get(), rssBeforeKb, rssAfterKb);
    }

    private static String queueSizeLabel(Integer queueSize, boolean noQueue) {
        return noQueue ? "none" : queueSize == null ? "default" : queueSize.toString();
    }

    private static void report(boolean virtual, String transport, int clients, int calls, long ioMs, int threads,
                               Integer queueSize, long[][] latenciesNs, long elapsedNs, int failures, int threadsBefore,
                               int peakThreads, int peakInFlight, long rssBeforeKb, long rssAfterKb) {
        long[] all = Arrays.stream(latenciesNs).flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
        int attempted = clients * calls;
        int completed = all.length;
        double elapsedS = elapsedNs / 1e9;
        // Throughput and latency percentiles are both computed over the calls that completed.
        double throughput = completed / elapsedS;
        double p50 = pct(all, 0.50);
        double p90 = pct(all, 0.90);
        double p99 = pct(all, 0.99);
        double max = all.length == 0 ? 0 : all[all.length - 1] / 1e6;
        // Handler concurrency: the configured pool with virtual threads or a queue size (simple transport), or with
        // TThreadPoolServer (digest); the platform-thread simple transport without a queue size falls back to
        // THsHaServer's own pool, whose effective concurrency is 5 (see SimpleTransportPlugin).
        int handlerConcurrency = virtual || queueSize != null || !transport.equals("simple") ? threads : 5;
        int concurrent = Math.min(clients, handlerConcurrency);
        // With io-ms 0 there is no I/O floor and the ideal is unbounded, so it is not reported.
        String ideal = ioMs == 0 ? "n/a" : String.format(Locale.ROOT, "%.0f", concurrent * 1000.0 / ioMs);
        // Platform threads created after the "before" sample are the client threads plus whatever the server
        // added (handlers, acceptor/selector threads). Subtracting the clients gives the server-side estimate.
        int serverThreadsEstimate = Math.max(0, peakThreads - threadsBefore - clients);

        String label = (virtual ? "virtual threads ON" : "virtual threads OFF") + ", " + transport + " transport";
        System.out.println();
        System.out.println("=== " + label + " ===");
        System.out.printf(Locale.ROOT, "  calls              : %d completed of %d attempted (%d clients failed)%n",
                          completed, attempted, failures);
        System.out.printf(Locale.ROOT, "  elapsed            : %.2f s%n", elapsedS);
        System.out.printf(Locale.ROOT, "  throughput         : %.0f calls/s (ideal at %d concurrent handlers: %s)%n",
                          throughput, concurrent, ideal);
        System.out.printf(Locale.ROOT, "  latency p50/p90/p99: %.1f / %.1f / %.1f ms (max %.1f, floor %d)%n",
                          p50, p90, p99, max, ioMs);
        System.out.printf(Locale.ROOT, "  peak handlers busy : %d%n", peakInFlight);
        System.out.printf(Locale.ROOT, "  platform threads   : %d before, %d peak (+%d)%n",
                          threadsBefore, peakThreads, peakThreads - threadsBefore);
        System.out.printf(Locale.ROOT, "  server threads     : ~%d (peak minus before minus %d client threads)%n",
                          serverThreadsEstimate, clients);
        System.out.printf(Locale.ROOT, "  RSS                : %d MB before, %d MB after%n",
                          rssBeforeKb / 1024, rssAfterKb / 1024);
        System.out.printf(Locale.ROOT,
                          "JSON {\"virtual\":%b,\"transport\":\"%s\",\"clients\":%d,\"calls\":%d,\"ioMs\":%d,"
                          + "\"threads\":%d,\"queueSize\":%s,"
                          + "\"throughput\":%.1f,\"p50Ms\":%.2f,\"p90Ms\":%.2f,\"p99Ms\":%.2f,\"maxMs\":%.2f,"
                          + "\"peakHandlersBusy\":%d,\"platformThreadsPeak\":%d,\"platformThreadsBefore\":%d,"
                          + "\"serverThreadsEstimate\":%d,"
                          + "\"rssBeforeMb\":%d,\"rssAfterMb\":%d,\"completedCalls\":%d,\"failedClients\":%d}%n",
                          virtual, transport, clients, calls, ioMs, threads, queueSize, throughput, p50, p90, p99, max,
                          peakInFlight, peakThreads, threadsBefore, serverThreadsEstimate,
                          rssBeforeKb / 1024, rssAfterKb / 1024, completed, failures);
    }

    private static NimbusClient connect(Map<String, Object> conf, int port, Semaphore connecting) throws Exception {
        connecting.acquire();
        try {
            return NimbusClient.Builder.withConf(conf).withTimeout(60_000).buildWithNimbusHostPort(HOST, port);
        } finally {
            connecting.release();
        }
    }

    private static double pct(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.min(sorted.length - 1, Math.round(q * (sorted.length - 1)));
        return sorted[idx] / 1e6;
    }

    /**
     * A Nimbus handler whose {@code getNimbusConf} blocks for {@code ioMs}; every other method is unsupported.
     */
    private static Nimbus.Iface sleepingHandler(long ioMs, AtomicInteger inFlight, AtomicInteger peakInFlight) {
        InvocationHandler h = (Object proxy, Method method, Object[] margs) -> {
            if (method.getName().equals("getNimbusConf")) {
                int now = inFlight.incrementAndGet();
                peakInFlight.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(ioMs);
                } finally {
                    inFlight.decrementAndGet();
                }
                return "{}";
            }
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == margs[0];
                    case "toString":
                        return "sleepingHandler(" + ioMs + "ms)";
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            }
            throw new UnsupportedOperationException(method.getName());
        };
        return (Nimbus.Iface) Proxy.newProxyInstance(Nimbus.Iface.class.getClassLoader(),
                                                     new Class<?>[]{ Nimbus.Iface.class }, h);
    }

    /**
     * Writes a JAAS configuration with a fixed digest user for the server and client sides. The login module class
     * is only named, never instantiated: Storm reads the user and password from the entry options.
     */
    private static Path writeDigestJaasConf() throws IOException {
        Path file = Files.createTempFile("storm-bench-jaas", ".conf");
        file.toFile().deleteOnExit();
        Files.writeString(file,
            "StormServer {\n"
            + "    org.apache.zookeeper.server.auth.DigestLoginModule required\n"
            + "    user_bench=\"benchsecret\";\n"
            + "};\n"
            + "StormClient {\n"
            + "    org.apache.zookeeper.server.auth.DigestLoginModule required\n"
            + "    username=\"bench\"\n"
            + "    password=\"benchsecret\";\n"
            + "};\n");
        return file;
    }

    /** Resident set size from /proc/self/status, or -1 when unavailable. */
    private static long rssKb() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", ""));
                }
            }
        } catch (IOException | RuntimeException e) {
            // not Linux, or unreadable
        }
        return -1;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + args[i]);
            }
            String key = args[i].substring(2);
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for --" + key);
            }
            opts.put(key, args[++i]);
        }
        return opts;
    }
}
