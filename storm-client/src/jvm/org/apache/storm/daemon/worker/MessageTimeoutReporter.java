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

import java.util.HexFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs the timeouts that the worker's spouts declare, when
 * {@link org.apache.storm.Config#TOPOLOGY_MESSAGE_TIMEOUT_REPORT} asks for it. A spout executor hands over a timeout
 * without blocking. The reporter's own thread writes the lines.
 */
public class MessageTimeoutReporter {

    /** Maximum length of an escaped message id, in UTF-16 code units. A cut id shares it with the truncation fields. */
    private static final int MAX_ID_UNITS = 1024;
    /** Maximum length of an escaped component id, in UTF-16 code units. */
    private static final int MAX_COMPONENT_UNITS = 64;
    /**
     * Bounds the memory of the queued events: a line has at most 1152 chars, so a full queue holds at most about
     * 2.4 MB. Timeouts that find it full are counted and not written. A cycle writes at most this many events.
     */
    static final int QUEUE_CAPACITY = 1024;
    static final long CYCLE_PERIOD_MS = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(MessageTimeoutReporter.class);

    private final Queue<String> lines = new ConcurrentLinkedQueue<>();
    /** A spout takes a slot before it builds a line, so a full queue costs it no formatting. */
    private final Semaphore freeSlots = new Semaphore(QUEUE_CAPACITY);
    private final LongAdder eventsDropped = new LongAdder();
    final Thread thread = new Thread(this::run, "message-timeout-reporter");
    /** Set by close() and when the thread ends: from then on the spouts hand nothing over. */
    private volatile boolean off;
    // the fields below belong to the reporter's thread
    private long writesFailed;
    private long droppedInLastSummary;
    private long failedInLastSummary;

    MessageTimeoutReporter() {
        thread.setDaemon(true);
        thread.setPriority(Thread.NORM_PRIORITY);
    }

    void start() {
        thread.start();
    }

    void close() {
        off = true;
        thread.interrupt();
    }

    /**
     * Records that the tuple tree {@code rootId} timed out. Task {@code taskId} of spout {@code componentId} in this
     * worker emitted it with {@code messageId}. Does not block. Calls no method of {@code messageId} unless it is a
     * {@code String}.
     */
    public void timedOut(long rootId, int taskId, String componentId, Object messageId) {
        if (off) {
            return;
        }
        if (freeSlots.tryAcquire()) {
            lines.add(eventLine(rootId, taskId, componentId, messageId));
        } else {
            eventsDropped.increment();
        }
    }

    int queuedEvents() {
        return QUEUE_CAPACITY - freeSlots.availablePermits();
    }

    /** An {@code Error} reaches the thread's uncaught-exception handler. */
    private void run() {
        try {
            while (!off) {
                runCycle();
                Time.sleep(CYCLE_PERIOD_MS);
            }
        } catch (InterruptedException e) {
            // close() interrupts the wait between two cycles
        } catch (RuntimeException e) {
            // turn the report off first: the line below can block or throw
            off = true;
            try {
                LOG.error("The message timeout report is off until the worker restarts", e);
            } catch (RuntimeException reportFailure) {
                // one attempt: the report stays off whether or not this line was written
            }
        } finally {
            // nothing reads the queue once this thread has ended
            off = true;
            lines.clear();
        }
    }

    /** Tests override it to make a cycle fail. */
    void runCycle() {
        for (int events = 0; events < QUEUE_CAPACITY; events++) {
            String line = lines.poll();
            if (line == null) {
                break;
            }
            freeSlots.release();
            write(line);
        }
        writeSummary();
    }

    private void writeSummary() {
        long dropped = eventsDropped.sum();
        long failed = writesFailed;
        long newlyDropped = dropped - droppedInLastSummary;
        long newlyFailed = failed - failedInLastSummary;
        if (newlyDropped == 0 && newlyFailed == 0) {
            return;
        }
        StringBuilder summary = new StringBuilder("summary");
        if (newlyDropped != 0) {
            summary.append(" events_dropped=").append(newlyDropped);
        }
        if (newlyFailed != 0) {
            summary.append(" write_failed=").append(newlyFailed);
        }
        if (write(summary.toString())) {
            droppedInLastSummary = dropped;
            failedInLastSummary = failed;
        }
    }

    private boolean write(String line) {
        try {
            LOG.info(line);
            return true;
        } catch (RuntimeException e) {
            writesFailed++;
            return false;
        }
    }

    /** Tests override it to count the lines built. */
    String eventLine(long rootId, int taskId, String componentId, Object messageId) {
        return "timeout " + rootToken(rootId) + " task=" + taskId + " " + componentField(componentId)
            + " " + idField(messageId);
    }

    /** Fixed width, so that a whole token is never the prefix of another root's token. */
    static String rootToken(long rootId) {
        return "root=" + HexFormat.of().toHexDigits(rootId);
    }

    /** Uses the escapes of the message id and also escapes a space. Nothing in the field marks a cut component id. */
    static String componentField(String componentId) {
        StringBuilder field = new StringBuilder("component=");
        appendEscapedPrefix(field, componentId, MAX_COMPONENT_UNITS, true);
        return field.toString();
    }

    /** Returns the {@code id} field of an event, after the truncation fields when the id is cut. */
    static String idField(Object messageId) {
        if (!(messageId instanceof String id)) {
            return "id=<not supported>";
        }
        StringBuilder whole = new StringBuilder("id=\"");
        if (appendEscapedPrefix(whole, id, MAX_ID_UNITS, false) == id.length()) {
            return whole.append('"').toString();
        }
        // the truncation fields take their share of the limit
        StringBuilder cut = new StringBuilder("truncated=true originalLength=").append(id.length());
        int unitsLeft = MAX_ID_UNITS - cut.length();
        cut.append(" id=\"");
        appendEscapedPrefix(cut, id, unitsLeft, false);
        return cut.append('"').toString();
    }

    /**
     * Appends the longest prefix of {@code text} whose escaped form takes at most {@code maxUnits} UTF-16 code units.
     * The prefix ends at a whole code point. Returns the number of code units of {@code text} in it.
     */
    private static int appendEscapedPrefix(StringBuilder out, String text, int maxUnits, boolean escapeSpace) {
        int end = out.length() + maxUnits;
        int i = 0;
        while (i < text.length()) {
            int codePoint = text.codePointAt(i);
            int before = out.length();
            appendEscaped(out, codePoint, escapeSpace);
            if (out.length() > end) {
                out.setLength(before);
                break;
            }
            i += Character.charCount(codePoint);
        }
        return i;
    }

    private static void appendEscaped(StringBuilder out, int codePoint, boolean escapeSpace) {
        if (codePoint == '\\') {
            out.append("\\\\");
            return;
        }
        if (escapeSpace && codePoint == ' ') {
            out.append("\\u{20}");
            return;
        }
        switch (Character.getType(codePoint)) {
            case Character.CONTROL, Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR,
                // codePointAt() returns a surrogate only when it is unpaired
                Character.SURROGATE -> out.append("\\u{").append(Integer.toHexString(codePoint)).append('}');
            default -> out.appendCodePoint(codePoint);
        }
    }
}
