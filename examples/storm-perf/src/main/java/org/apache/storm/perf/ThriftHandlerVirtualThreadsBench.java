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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.security.auth.SimpleTransportPlugin;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * Benchmark for {@code storm.virtual.threads.enabled} on the Nimbus Thrift handler pool.
 *
 * <p>Starts an in-process {@link ThriftServer} using {@link SimpleTransportPlugin} whose {@code getNimbusConf}
 * handler sleeps for a configurable time to simulate blocking I/O (ZooKeeper, blob store), then hammers it with
 * N client threads and reports throughput, latency percentiles, peak platform-thread count and RSS. Run it with
 * the flag off and on to compare.
 *
 * <p>Build and run (JDK 25, Maven 3.9):
 * <pre>
 * mvn -pl storm-client,examples/storm-perf -am install -DskipTests -Dcheckstyle.skip=true -q
 * mvn -pl examples/storm-perf dependency:build-classpath -Dmdep.outputFile=target/cp.txt -q
 * java -cp examples/storm-perf/target/classes:$(cat examples/storm-perf/target/cp.txt) \
 *     org.apache.storm.perf.ThriftHandlerVirtualThreadsBench --mode both --clients 200 --calls 50 --io-ms 10
 * </pre>
 *
 * <p>Options: {@code --mode off|on|both}, {@code --clients N}, {@code --calls M} (per client),
 * {@code --io-ms L} (simulated handler I/O), {@code --threads T} ({@code nimbus.thrift.threads}),
 * {@code --queue-size Q|none} ({@code nimbus.queue.size}; defaults to the shipped 100000, so both modes use a pool of
 * {@code --threads} handlers; {@code none} removes the key so the platform-thread run falls back to THsHaServer's own
 * pool, whose effective concurrency is 5), {@code --warmup W} (calls per client excluded from statistics).
 *
 * <p>For comparable RSS numbers run each mode in its own JVM ({@code --mode off} then {@code --mode on}).
 */
public final class ThriftHandlerVirtualThreadsBench {

    private static final String HOST = "localhost";

    private ThriftHandlerVirtualThreadsBench() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String mode = opts.getOrDefault("mode", "both").toLowerCase(Locale.ROOT);
        int clients = Integer.parseInt(opts.getOrDefault("clients", "200"));
        int calls = Integer.parseInt(opts.getOrDefault("calls", "50"));
        long ioMs = Long.parseLong(opts.getOrDefault("io-ms", "10"));
        int threads = Integer.parseInt(opts.getOrDefault("threads", "64"));
        String queueOpt = opts.get("queue-size");
        boolean noQueue = "none".equalsIgnoreCase(queueOpt);
        Integer queueSize = queueOpt == null || noQueue ? null : Integer.valueOf(queueOpt);
        int warmup = Integer.parseInt(opts.getOrDefault("warmup", "5"));

        System.out.printf(Locale.ROOT, "clients=%d calls=%d io-ms=%d threads=%d queue-size=%s warmup=%d%n",
                          clients, calls, ioMs, threads, noQueue ? "none" : queueSize, warmup);
        if (mode.equals("off") || mode.equals("both")) {
            runPhase(false, clients, calls, ioMs, threads, queueSize, noQueue, warmup);
        }
        if (mode.equals("on") || mode.equals("both")) {
            runPhase(true, clients, calls, ioMs, threads, queueSize, noQueue, warmup);
        }
    }

    private static void runPhase(boolean virtual, int clients, int calls, long ioMs, int threads,
                                 Integer queueSize, boolean noQueue, int warmup) throws Exception {
        Map<String, Object> conf = new HashMap<>(Utils.readDefaultConfig());
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, SimpleTransportPlugin.class.getName());
        conf.put(Config.NIMBUS_THRIFT_PORT, 0);
        conf.put(Config.NIMBUS_THRIFT_THREADS, threads);
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, virtual);
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
        if (noQueue) {
            conf.remove(Config.NIMBUS_QUEUE_SIZE);
        } else if (queueSize != null) {
            conf.put(Config.NIMBUS_QUEUE_SIZE, queueSize);
        }

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peakInFlight = new AtomicInteger();
        Nimbus.Iface handler = sleepingHandler(ioMs, inFlight, peakInFlight);
        ThriftServer server = new ThriftServer(conf, new Nimbus.Processor<>(handler), ThriftConnectionType.NIMBUS);
        Thread serveThread = new Thread(server::serve, "bench-thrift-serve");
        serveThread.setDaemon(true);
        serveThread.start();
        while (!server.isServing()) {
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
        for (int c = 0; c < clients; c++) {
            final int clientIdx = c;
            Thread t = new Thread(() -> {
                try (NimbusClient client = NimbusClient.Builder.withConf(conf).withTimeout(60_000)
                        .buildWithNimbusHostPort(HOST, port)) {
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

        report(virtual, clients, calls, ioMs, threads, queueSize, latenciesNs, elapsedNs, failures.size(),
               threadsBefore, peakThreads.get(), peakInFlight.get(), rssBeforeKb, rssAfterKb);
    }

    private static void report(boolean virtual, int clients, int calls, long ioMs, int threads, Integer queueSize,
                               long[][] latenciesNs, long elapsedNs, int failures, int threadsBefore, int peakThreads,
                               int peakInFlight, long rssBeforeKb, long rssAfterKb) {
        long[] all = Arrays.stream(latenciesNs).flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
        int total = clients * calls;
        double elapsedS = elapsedNs / 1e9;
        double throughput = total / elapsedS;
        double p50 = pct(all, 0.50);
        double p90 = pct(all, 0.90);
        double p99 = pct(all, 0.99);
        double max = all.length == 0 ? 0 : all[all.length - 1] / 1e6;
        double idealThroughput = (double) Math.min(clients, threads) * 1000.0 / ioMs;

        String label = virtual ? "virtual threads ON" : "virtual threads OFF";
        System.out.println();
        System.out.println("=== " + label + " ===");
        System.out.printf(Locale.ROOT, "  calls              : %d (%d failed)%n", total, failures);
        System.out.printf(Locale.ROOT, "  elapsed            : %.2f s%n", elapsedS);
        System.out.printf(Locale.ROOT, "  throughput         : %.0f calls/s (ideal at %d concurrent handlers: %.0f)%n",
                          throughput, Math.min(clients, threads), idealThroughput);
        System.out.printf(Locale.ROOT, "  latency p50/p90/p99: %.1f / %.1f / %.1f ms (max %.1f, floor %d)%n",
                          p50, p90, p99, max, ioMs);
        System.out.printf(Locale.ROOT, "  peak handlers busy : %d%n", peakInFlight);
        System.out.printf(Locale.ROOT, "  platform threads   : %d before, %d peak (+%d)%n",
                          threadsBefore, peakThreads, peakThreads - threadsBefore);
        System.out.printf(Locale.ROOT, "  RSS                : %d MB before, %d MB after%n",
                          rssBeforeKb / 1024, rssAfterKb / 1024);
        System.out.printf(Locale.ROOT,
                          "JSON {\"virtual\":%b,\"clients\":%d,\"calls\":%d,\"ioMs\":%d,\"threads\":%d,\"queueSize\":%s,"
                          + "\"throughput\":%.1f,\"p50Ms\":%.2f,\"p90Ms\":%.2f,\"p99Ms\":%.2f,\"maxMs\":%.2f,"
                          + "\"peakHandlersBusy\":%d,\"platformThreadsPeak\":%d,\"platformThreadsBefore\":%d,"
                          + "\"rssBeforeMb\":%d,\"rssAfterMb\":%d,\"failures\":%d}%n",
                          virtual, clients, calls, ioMs, threads, queueSize, throughput, p50, p90, p99, max,
                          peakInFlight, peakThreads, threadsBefore, rssBeforeKb / 1024, rssAfterKb / 1024, failures);
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
                return method.invoke(inFlight, margs);
            }
            throw new UnsupportedOperationException(method.getName());
        };
        return (Nimbus.Iface) Proxy.newProxyInstance(Nimbus.Iface.class.getClassLoader(),
                                                     new Class<?>[]{ Nimbus.Iface.class }, h);
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
