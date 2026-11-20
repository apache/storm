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

package org.apache.storm.daemon.worker;

import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class MessageTimeoutReporterTest {

    private final InstrumentedMessageTimeoutReporter reporter = new InstrumentedMessageTimeoutReporter();
    /** What reached the default uncaught-exception handler, with its thread. */
    private final BlockingQueue<Map.Entry<Thread, Throwable>> uncaught = new LinkedBlockingQueue<>();
    private Thread.UncaughtExceptionHandler previousHandler;
    private MessageTimeoutReportLog log;
    private SimulatedTime simulatedTime;

    @BeforeEach
    public void setUp() {
        simulatedTime = new SimulatedTime();
        log = MessageTimeoutReportLog.attach();
        previousHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, thrown) -> uncaught.add(Map.entry(thread, thrown)));
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        log.release();
        reporter.close();
        // the thread may still write what it had queued: wait for it to end before the next test attaches its log
        reporter.awaitThreadEnd();
        log.close();
        simulatedTime.close();
        Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }

    @Test
    public void testATimeoutIsWrittenOnceByTheReportersThread() throws Exception {
        reporter.start();

        reporter.timedOut(123456789L, 7, "url-spout", "https://example.org/a");
        advanceUntil(() -> log.written.size() == 1);
        awaitACycleStartedFromNow();

        assertEquals(List.of("timeout root=00000000075bcd15 task=7 component=url-spout id=\"https://example.org/a\""), log.messages());
        LogEvent event = log.written.get(0);
        assertEquals(Level.INFO, event.getLevel());
        assertEquals(MessageTimeoutReportLog.LOGGER_NAME, event.getLoggerName());
        assertSame(reporter.thread, log.writers.get(0));
    }

    @Test
    public void testTheThreadIsADaemonAtNormalPriorityWhateverThePriorityOfItsCreator() {
        Thread creator = Thread.currentThread();
        int priority = creator.getPriority();
        creator.setPriority(Thread.MAX_PRIORITY);
        Thread thread;
        try {
            thread = new InstrumentedMessageTimeoutReporter().thread;
        } finally {
            creator.setPriority(priority);
        }

        assertEquals(Thread.NORM_PRIORITY, thread.getPriority());
        assertTrue(thread.isDaemon());
        assertEquals("message-timeout-reporter", thread.getName());
    }

    @Test
    public void testAMessageIdThatIsNotAStringIsNotCalled() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Object messageId = new Object() {
            @Override
            public String toString() {
                calls.incrementAndGet();
                return "id";
            }

            @Override
            public int hashCode() {
                calls.incrementAndGet();
                return 1;
            }

            @Override
            public boolean equals(Object other) {
                calls.incrementAndGet();
                return other == this;
            }
        };
        reporter.start();

        reporter.timedOut(123456789L, 7, "url-spout", messageId);
        advanceUntil(() -> log.written.size() == 1);

        assertEquals(List.of("timeout root=00000000075bcd15 task=7 component=url-spout id=<not supported>"),
            log.messages());
        assertEquals(0, calls.get());
    }

    @Test
    public void testAFullQueueDropsBeforeFormattingAndTheDropsStayCountedUntilASummaryIsWritten() throws Exception {
        reporter.start();

        fillTheQueueBehindAHeldWrite();
        int formatted = reporter.linesFormatted();
        for (int i = 0; i < 3; i++) {
            reporter.timedOut(-1L, "dropped");
        }

        assertEquals(formatted, reporter.linesFormatted());
        assertEquals(MessageTimeoutReporter.QUEUE_CAPACITY, reporter.queuedEvents());

        // the first summary that reports the drops fails; a later cycle writes it
        log.summaryWritesToFail.set(1);
        log.release();
        int events = MessageTimeoutReporter.QUEUE_CAPACITY + 1;
        advanceUntil(() -> log.written.size() == events + 1);
        awaitACycleStartedFromNow();

        assertEquals(List.of("summary events_dropped=3 write_failed=1"), summaries());
        assertEquals(events + 1, log.written.size());
        assertTrue(log.messages().stream().noneMatch(line -> line.contains("dropped\"")));
        assertEquals(0, reporter.queuedEvents());

        // a second burst: its summary counts from the previous one
        fillTheQueueBehindAHeldWrite();
        reporter.timedOut(-1L, "dropped");
        reporter.timedOut(-1L, "dropped");
        log.release();
        advanceUntil(() -> log.written.size() == 2 * (events + 1));

        // a cycle writes as many events as the queue holds, then the summary; the last event waits for the next cycle
        List<String> messages = log.messages();
        assertEquals("summary events_dropped=2", messages.get(messages.size() - 2));
        assertTrue(messages.get(messages.size() - 1).startsWith("timeout "));
    }

    @Test
    public void testAWriteThatThrowsStaysCountedUntilASummaryIsWritten() throws Exception {
        // the event, then the summary that would have reported it
        log.writesToFail.set(2);
        reporter.start();

        reporter.timedOut(1L, "not written");
        advanceUntil(() -> log.written.size() == 1);
        awaitACycleStartedFromNow();

        assertEquals(List.of("summary write_failed=2"), log.messages());
    }

    @Test
    public void testAQueuedEventDoesNotKeepTheMessageIdReachable() throws Exception {
        reporter.start();
        log.hold();
        reporter.timedOut(0L, "held in the write");
        advanceUntil(() -> log.writesAtTheGate.tryAcquire());

        String messageId = "https://example.org/" + System.nanoTime();
        WeakReference<String> reference = new WeakReference<>(messageId);
        reporter.timedOut(1L, messageId);
        messageId = null;

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(InstrumentedMessageTimeoutReporter.WAIT_SECS);
        while (reference.get() != null && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(10);
        }
        assertNull(reference.get());
        assertEquals(1, reporter.queuedEvents());
    }

    @Test
    public void testARuntimeExceptionInACycleTurnsTheReportOffBeforeItIsReported() throws Exception {
        reporter.start();
        reporter.timedOut(1L, "before");
        advanceUntil(() -> log.written.size() == 1);

        log.hold();
        reporter.failNextCycle(new IllegalStateException("injected"));
        advanceUntil(() -> log.writesAtTheGate.tryAcquire());
        // the report at ERROR is still inside its write
        reporter.timedOut(2L, "after");
        assertEquals(0, reporter.queuedEvents());
        assertEquals(1, reporter.linesFormatted());

        log.release();
        assertTrue(reporter.awaitThreadEnd());
        assertEquals(2, log.written.size());
        LogEvent report = log.written.get(1);
        assertEquals(Level.ERROR, report.getLevel());
        assertEquals("The message timeout report is off until the worker restarts",
            report.getMessage().getFormattedMessage());
        assertEquals("injected", report.getThrown().getMessage());
        assertTrue(uncaught.isEmpty());
    }

    @Test
    public void testTheReportStaysOffWhenTheErrorCannotBeWritten() throws Exception {
        log.writesToFail.set(1);
        reporter.failNextCycle(new IllegalStateException("injected"));
        reporter.start();

        advanceUntil(() -> log.writers.size() == 1);
        assertTrue(reporter.awaitThreadEnd());
        reporter.timedOut(1L, "after");

        assertEquals(0, reporter.queuedEvents());
        assertEquals(List.of(), log.messages());
        assertTrue(uncaught.isEmpty());
    }

    @Test
    public void testAnErrorInACycleReachesTheUncaughtExceptionHandler() throws Exception {
        for (Error error : List.of(new OutOfMemoryError("injected"), new LinkageError("injected"))) {
            InstrumentedMessageTimeoutReporter failing = new InstrumentedMessageTimeoutReporter();
            failing.failNextCycle(error);
            failing.start();

            Map.Entry<Thread, Throwable> reached = uncaught.poll(InstrumentedMessageTimeoutReporter.WAIT_SECS, TimeUnit.SECONDS);
            assertSame(failing.thread, reached.getKey());
            assertSame(error, reached.getValue());
            assertTrue(failing.awaitThreadEnd());
            failing.timedOut(1L, "after");
            assertEquals(0, failing.queuedEvents());
        }
        assertEquals(List.of(), log.messages());
    }

    @Test
    public void testCloseAndAnInterruptEndTheThreadWithoutAFailureReportAndTurnTheReportOff() throws Exception {
        List<Consumer<MessageTimeoutReporter>> stops = List.of(MessageTimeoutReporter::close, r -> r.thread.interrupt());
        for (Consumer<MessageTimeoutReporter> stop : stops) {
            InstrumentedMessageTimeoutReporter stopped = new InstrumentedMessageTimeoutReporter();
            stopped.start();
            advanceUntil(() -> stopped.cyclesEnded() > 0);

            stop.accept(stopped);

            assertTrue(stopped.awaitThreadEnd());
            stopped.timedOut(1L, "after");
            assertEquals(0, stopped.queuedEvents());
        }
        assertEquals(List.of(), log.messages());
    }

    @Test
    public void testComponentFieldIsEscapedAndCutAtSixtyFourUnits() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("url-spout", "component=url-spout");
        // a space would let the component id read as another field of the line
        fields.put("url spout", "component=url\\u{20}spout");
        fields.put("s truncated=true", "component=s\\u{20}truncated=true");
        fields.put("a\nb", "component=a\\u{a}b");
        fields.put("c".repeat(64), "component=" + "c".repeat(64));
        fields.put("c".repeat(65), "component=" + "c".repeat(64));
        // the 64 units are counted after the escapes: 32 backslashes take them all
        fields.put("\\".repeat(40), "component=" + "\\\\".repeat(32));
        // an escape is kept whole or left out
        fields.put("c".repeat(58) + " ", "component=" + "c".repeat(58) + "\\u{20}");
        fields.put("c".repeat(59) + " ", "component=" + "c".repeat(59));

        fields.forEach((component, field) -> assertEquals(field, MessageTimeoutReporter.componentField(component)));
    }

    /** The length that the comment of {@link MessageTimeoutReporter#QUEUE_CAPACITY} gives for the longest line. */
    @Test
    public void testTheLongestEventLineHas1152Chars() {
        String line = reporter.eventLine(Long.MIN_VALUE, Integer.MIN_VALUE, "c".repeat(100), "a".repeat(5000));

        assertEquals("timeout root=8000000000000000 task=-2147483648 component=" + "c".repeat(64)
            + " truncated=true originalLength=5000 id=\"" + "a".repeat(990) + "\"", line);
        assertEquals(1152, line.length());
    }

    @Test
    public void testRootTokenIsSixteenHexDigits() {
        Map<Long, String> tokens = new LinkedHashMap<>();
        tokens.put(0L, "root=0000000000000000");
        tokens.put(1L, "root=0000000000000001");
        tokens.put(-1L, "root=ffffffffffffffff");
        tokens.put(Long.MIN_VALUE, "root=8000000000000000");
        tokens.put(123456789L, "root=00000000075bcd15");
        // unpadded, "12" is a prefix of "123"
        tokens.put(0x12L, "root=0000000000000012");
        tokens.put(0x123L, "root=0000000000000123");

        tokens.forEach((root, token) -> assertEquals(token, MessageTimeoutReporter.rootToken(root)));
    }

    @Test
    public void testIdFieldEscapesBackslashControlFormatAndSeparatorCharacters() {
        String grinningFace = Character.toString(0x1F600);
        String languageTag = Character.toString(0xE0001);
        Map<Object, String> fields = new LinkedHashMap<>();
        fields.put("https://example.org/a", "id=\"https://example.org/a\"");
        fields.put("a b\u00a0c", "id=\"a b\u00a0c\"");
        fields.put("say \"hi\"", "id=\"say \"hi\"\"");
        fields.put("a" + grinningFace + "b", "id=\"a" + grinningFace + "b\"");
        fields.put("a\\b", "id=\"a\\\\b\"");
        fields.put("a\nb\rc", "id=\"a\\u{a}b\\u{d}c\"");
        fields.put("a\tb\u0000c\u007f", "id=\"a\\u{9}b\\u{0}c\\u{7f}\"");
        fields.put("next\u0085line", "id=\"next\\u{85}line\"");
        fields.put("a\u2028b\u2029c", "id=\"a\\u{2028}b\\u{2029}c\"");
        fields.put("right\u202eto\u200eleft", "id=\"right\\u{202e}to\\u{200e}left\"");
        fields.put("zero\u200dwidth\u00adsoft\ufeff", "id=\"zero\\u{200d}width\\u{ad}soft\\u{feff}\"");
        fields.put("a" + languageTag + "b", "id=\"a\\u{e0001}b\"");
        fields.put("a\ud800b", "id=\"a\\u{d800}b\"");
        fields.put("a\udc00\ud800", "id=\"a\\u{dc00}\\u{d800}\"");
        fields.put(42, "id=<not supported>");

        fields.forEach((messageId, field) -> assertEquals(field, MessageTimeoutReporter.idField(messageId)));
    }

    @Test
    public void testIdFieldIsCutAtTheLimitWithoutSplittingACodePointOrAnEscape() {
        String grinningFace = Character.toString(0x1F600);
        Map<String, String> fields = new LinkedHashMap<>();
        // 1024 units after the escapes: written whole
        fields.put("a".repeat(1024), "id=\"" + "a".repeat(1024) + "\"");
        fields.put("a".repeat(1019) + "\n", "id=\"" + "a".repeat(1019) + "\\u{a}\"");
        fields.put("a".repeat(1022) + grinningFace, "id=\"" + "a".repeat(1022) + grinningFace + "\"");
        // one unit more: the two fields take 34 units and leave 990
        fields.put("a".repeat(1025), "truncated=true originalLength=1025 id=\"" + "a".repeat(990) + "\"");
        fields.put("a".repeat(1020) + "\n", "truncated=true originalLength=1021 id=\"" + "a".repeat(990) + "\"");
        fields.put("a".repeat(10000), "truncated=true originalLength=10000 id=\"" + "a".repeat(989) + "\"");
        // a space stays literal in a cut id too
        fields.put("a ".repeat(600), "truncated=true originalLength=1200 id=\"" + "a ".repeat(495) + "\"");
        // 33 units for the fields leave 991: 495 escaped backslashes, and one unit that half an escape cannot use
        fields.put("\\".repeat(600), "truncated=true originalLength=600 id=\"" + "\\\\".repeat(495) + "\"");
        // unit 990 falls inside the surrogate pair, then inside the escape
        fields.put("a".repeat(989) + grinningFace + "a".repeat(100),
            "truncated=true originalLength=1091 id=\"" + "a".repeat(989) + "\"");
        fields.put("a".repeat(988) + "\n" + "a".repeat(100),
            "truncated=true originalLength=1089 id=\"" + "a".repeat(988) + "\"");
        // the escape ends at unit 990 and is kept
        fields.put("a".repeat(985) + "\n" + "a".repeat(100),
            "truncated=true originalLength=1086 id=\"" + "a".repeat(985) + "\\u{a}\"");

        fields.forEach((messageId, field) -> assertEquals(field, MessageTimeoutReporter.idField(messageId)));
    }

    /** Advances simulated time one cycle period at a time until {@code done} holds. */
    private static void advanceUntil(BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(InstrumentedMessageTimeoutReporter.WAIT_SECS);
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "not reached within " + InstrumentedMessageTimeoutReporter.WAIT_SECS + " s");
            Time.advanceTime(MessageTimeoutReporter.CYCLE_PERIOD_MS);
            Thread.sleep(20);
        }
    }

    /**
     * Returns once a cycle that started after this call has ended. The first cycle to end may have started before the
     * call; the second one cannot.
     */
    private void awaitACycleStartedFromNow() throws InterruptedException {
        int ended = reporter.cyclesEnded();
        advanceUntil(() -> reporter.cyclesEnded() >= ended + 2);
    }

    /** Holds the reporter's thread inside the write of one event, then queues events until the queue is full. */
    private void fillTheQueueBehindAHeldWrite() throws InterruptedException {
        log.hold();
        reporter.timedOut(0L, "held in the write");
        advanceUntil(() -> log.writesAtTheGate.tryAcquire());
        for (int i = 1; i <= MessageTimeoutReporter.QUEUE_CAPACITY; i++) {
            reporter.timedOut(i, "queued");
        }
        assertEquals(MessageTimeoutReporter.QUEUE_CAPACITY, reporter.queuedEvents());
    }

    private List<String> summaries() {
        return log.messages().stream().filter(line -> line.startsWith("summary")).collect(Collectors.toList());
    }
}
