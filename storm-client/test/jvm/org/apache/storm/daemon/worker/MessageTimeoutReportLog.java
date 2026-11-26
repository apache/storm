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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

/**
 * For tests: records what the logger of the message timeout report writes, from INFO up. A test can hold the writes at
 * a gate or make the next ones throw.
 */
public final class MessageTimeoutReportLog extends AbstractAppender implements AutoCloseable {

    public static final String LOGGER_NAME = "org.apache.storm.daemon.worker.MessageTimeoutReporter";

    /** The events whose write returned, in order. */
    public final List<LogEvent> written = new CopyOnWriteArrayList<>();
    /** The thread of each write that started. */
    public final List<Thread> writers = new CopyOnWriteArrayList<>();
    /** One permit for each write that reached the gate while it was held. */
    public final Semaphore writesAtTheGate = new Semaphore(0);
    /** How many of the next writes throw. */
    public final AtomicInteger writesToFail = new AtomicInteger();
    /** How many of the next writes of a summary line throw. */
    public final AtomicInteger summaryWritesToFail = new AtomicInteger();
    private volatile CountDownLatch gate;

    private MessageTimeoutReportLog() {
        // ignoreExceptions is false, so that an exception from append() reaches the caller of the logger
        super("message-timeout-report-log", null, null, false, Property.EMPTY_ARRAY);
    }

    public static MessageTimeoutReportLog attach() {
        MessageTimeoutReportLog log = new MessageTimeoutReportLog();
        log.start();
        LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, Level.INFO, false);
        loggerConfig.addAppender(log, null, null);
        LoggerContext context = LoggerContext.getContext(false);
        context.getConfiguration().addLogger(LOGGER_NAME, loggerConfig);
        context.updateLoggers();
        return log;
    }

    @Override
    public void close() {
        release();
        LoggerContext context = LoggerContext.getContext(false);
        context.getConfiguration().removeLogger(LOGGER_NAME);
        context.updateLoggers();
    }

    /** Makes every write wait at the gate until {@link #release()}. */
    public void hold() {
        gate = new CountDownLatch(1);
    }

    public void release() {
        CountDownLatch held = gate;
        gate = null;
        if (held != null) {
            held.countDown();
        }
    }

    public List<String> messages() {
        return written.stream().map(e -> e.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    @Override
    public void append(LogEvent event) {
        writers.add(Thread.currentThread());
        boolean summary = event.getMessage().getFormattedMessage().startsWith("summary");
        if (writesToFail.getAndUpdate(n -> Math.max(0, n - 1)) > 0
            || summary && summaryWritesToFail.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new IllegalStateException("the write failed");
        }
        CountDownLatch held = gate;
        if (held != null) {
            writesAtTheGate.release();
            try {
                held.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        written.add(event.toImmutable());
    }
}
