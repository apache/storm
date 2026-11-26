/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.storm.ILocalCluster.ILocalTopology;
import org.apache.storm.daemon.worker.MessageTimeoutReportLog;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs a spout whose message ids are URLs, and a bolt that fails or holds its tuples, on a one-worker local cluster
 * under simulated time, and reads the worker's message timeout report from its logger.
 */
public class MessageTimeoutReportTest {

    private static final String REPORTER_THREAD = "message-timeout-reporter";
    private static final String FAILED_URL = "https://example.org/failed";
    private static final String HELD_URL = "https://example.org/held";
    private static final String SECOND_HELD_URL = "https://example.org/held-2";
    /** The root id of the tuple tree of each URL, as the bolt received it. */
    private static final Map<String, Long> ROOT_BY_URL = new ConcurrentHashMap<>();
    /** The spout task that emitted each URL, as the bolt received it. */
    private static final Map<String, Integer> SPOUT_TASK_BY_URL = new ConcurrentHashMap<>();
    private static final AtomicInteger FIRST_SPOUT_TASK = new AtomicInteger();
    /** The threads that opened the spout's tasks: one, when the two tasks share an executor. */
    private static final Set<Thread> SPOUT_THREADS = ConcurrentHashMap.newKeySet();

    private LocalCluster cluster;
    private MessageTimeoutReportLog log;
    private FeederSpout spout;
    private AckFailMapTracker tracker;

    @BeforeEach
    public void startCluster() throws Exception {
        // a worker of an earlier test may still be stopping
        Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> !reporterThreadIsAlive());
        ROOT_BY_URL.clear();
        SPOUT_TASK_BY_URL.clear();
        SPOUT_THREADS.clear();
        log = MessageTimeoutReportLog.attach();
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(1)
            .withDaemonConf(Map.of(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true))
            .build();
    }

    @AfterEach
    public void stopCluster() throws Exception {
        log.release();
        cluster.close();
        log.close();
    }

    @Test
    public void testATimeoutIsReportedWithItsRootTaskAndUrlAndAnExplicitFailIsNot() throws Exception {
        ILocalTopology topology = submit("timeout-events");

        spout.feed(new Values(FAILED_URL), FAILED_URL);
        // in real time: the cluster's time stands still, so this tuple cannot time out before its bolt fails it
        Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> tracker.isFailed(FAILED_URL));
        // had the explicit fail queued an event, it would be written before the event of the tuple fed after it
        spout.feed(new Values(HELD_URL), HELD_URL);
        advanceUntil(() -> tracker.isFailed(HELD_URL));
        advanceUntil(() -> !log.written.isEmpty());

        assertEquals(List.of(timeoutEvent(HELD_URL)), log.messages());
        assertNotEquals(FIRST_SPOUT_TASK.get(), SPOUT_TASK_BY_URL.get(HELD_URL));
        assertEquals(1, SPOUT_THREADS.size());
        assertEquals(REPORTER_THREAD, log.writers.get(0).getName());

        topology.close();
        advanceUntil(() -> !reporterThreadIsAlive());
        assertEquals(List.of(timeoutEvent(HELD_URL)), log.messages());
    }

    @Test
    public void testNothingIsReportedAndNoThreadRunsByDefault() throws Exception {
        submit(null);

        spout.feed(new Values(HELD_URL), HELD_URL);
        advanceUntil(() -> tracker.isFailed(HELD_URL));

        // with no reporter thread, no cycle can write a line later
        assertFalse(reporterThreadIsAlive());
        assertEquals(List.of(), log.messages());
    }

    @Test
    public void testTheSpoutIsStillFailedWhileTheLogIsBlockedForLongerThanTheTimeout() throws Exception {
        submit("timeout-events");
        log.hold();

        spout.feed(new Values(HELD_URL), HELD_URL);
        advanceUntil(() -> tracker.isFailed(HELD_URL));
        advanceUntil(() -> log.writesAtTheGate.tryAcquire());
        // the reporter is now inside the write of the first event; this tuple is emitted and times out meanwhile
        spout.feed(new Values(SECOND_HELD_URL), SECOND_HELD_URL);
        advanceUntil(() -> tracker.isFailed(SECOND_HELD_URL));
        assertEquals(List.of(), log.messages());

        log.release();
        advanceUntil(() -> log.written.size() == 2);
        assertEquals(List.of(timeoutEvent(HELD_URL), timeoutEvent(SECOND_HELD_URL)), log.messages());
    }

    /** Submits the topology, with the report in {@code reportMode} or, for null, with the key left out. */
    private ILocalTopology submit(String reportMode) throws Exception {
        spout = new SecondTaskFeederSpout();
        tracker = new AckFailMapTracker();
        spout.setAckFailDelegate(tracker);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1).setNumTasks(2);
        builder.setBolt("bolt", new FailOrHoldBolt()).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setMessageTimeoutSecs(10);
        if (reportMode != null) {
            conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_REPORT, reportMode);
        }
        return cluster.submitTopology("message-timeout-report", conf, builder.createTopology());
    }

    private static String timeoutEvent(String url) {
        return String.format("timeout root=%016x task=%d component=spout id=\"%s\"",
            ROOT_BY_URL.get(url), SPOUT_TASK_BY_URL.get(url), url);
    }

    /** Advances the cluster's time one second at a time until {@code done} holds. */
    private void advanceUntil(BooleanSupplier done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Testing.TEST_TIMEOUT_MS);
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "not reached within " + Testing.TEST_TIMEOUT_MS + " ms");
            cluster.advanceClusterTime(1);
        }
    }

    private static boolean reporterThreadIsAlive() {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(thread -> REPORTER_THREAD.equals(thread.getName()));
    }

    /**
     * A feeder spout for one executor of two tasks. Only the second task emits, so an event that named the executor's
     * first task would not match the source task of the tuple.
     */
    private static class SecondTaskFeederSpout extends FeederSpout {
        private transient boolean emits;

        SecondTaskFeederSpout() {
            super(new Fields("url"));
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            emits = context.getThisTaskIndex() == 1;
            SPOUT_THREADS.add(Thread.currentThread());
            if (!emits) {
                FIRST_SPOUT_TASK.set(context.getThisTaskId());
            }
        }

        @Override
        public void nextTuple() {
            if (emits) {
                super.nextTuple();
            }
        }
    }

    /** Fails the tuple of {@link #FAILED_URL}; neither acks nor fails the others, so their trees time out. */
    private static class FailOrHoldBolt extends BaseRichBolt {
        private transient OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String url = input.getString(0);
            ROOT_BY_URL.put(url, input.getMessageId().getAnchors().iterator().next());
            SPOUT_TASK_BY_URL.put(url, input.getSourceTask());
            if (FAILED_URL.equals(url)) {
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
