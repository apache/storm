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

package org.apache.storm.windowing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.storm.topology.FailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks tuples across input streams and periodically emits watermark events. Watermark event timestamp is the minimum of the latest tuple
 * timestamps across all the input streams (minus the lag). Once a watermark event is emitted any tuple coming with an earlier timestamp can
 * be considered as late events.
 */
public class WaterMarkEventGenerator<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WaterMarkEventGenerator.class);
    private final WindowManager<T> windowManager;
    private final int eventTsLag;
    private final Set<GlobalStreamId> inputStreams;
    private final Map<GlobalStreamId, Long> streamToTs;
    private final ScheduledExecutorService executorService;
    private final int interval;
    private ScheduledFuture<?> executorFuture;
    private volatile long lastWaterMarkTs;

    /**
     * Creates a new WatermarkEventGenerator.
     *
     * @param windowManager The window manager this generator will submit watermark events to
     * @param intervalMs    The generator will check if it should generate a watermark event with this interval
     * @param eventTsLagMs  The max allowed lag behind the last watermark event before an event is considered late
     * @param inputStreams  The input streams this generator is expected to handle
     */
    public WaterMarkEventGenerator(WindowManager<T> windowManager, int intervalMs,
                                   int eventTsLagMs, Set<GlobalStreamId> inputStreams) {
        this.windowManager = windowManager;
        streamToTs = new ConcurrentHashMap<>();

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("watermark-event-generator-%d")
            .setDaemon(true)
            .build();
        executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

        this.interval = intervalMs;
        this.eventTsLag = eventTsLagMs;
        this.inputStreams = inputStreams;
    }

    /**
     * Tracks the timestamp of the event in the stream, returns true if the event can be considered for processing or false if its a late
     * event.
     */
    public boolean track(GlobalStreamId stream, long ts) {
        Long currentVal = streamToTs.get(stream);
        if (currentVal == null || ts > currentVal) {
            streamToTs.put(stream, ts);
        }
        checkFailures();
        return ts >= lastWaterMarkTs;
    }

    @Override
    public void run() {
        try {
            long waterMarkTs = computeWaterMarkTs();
            if (waterMarkTs > lastWaterMarkTs) {
                this.windowManager.add(new WaterMarkEvent<>(waterMarkTs));
                lastWaterMarkTs = waterMarkTs;
            }
        } catch (Throwable th) {
            LOG.error("Failed while processing watermark event ", th);
            throw th;
        }
    }

    /**
     * Computes the min ts across all streams.
     */
    private long computeWaterMarkTs() {
        long ts = 0;
        // only if some data has arrived on each input stream
        if (streamToTs.size() >= inputStreams.size()) {
            ts = Long.MAX_VALUE;
            for (Map.Entry<GlobalStreamId, Long> entry : streamToTs.entrySet()) {
                ts = Math.min(ts, entry.getValue());
            }
        }
        return ts - eventTsLag;
    }

    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException ex) {
                LOG.error("Got exception ", ex);
                throw new FailedException(ex);
            } catch (ExecutionException ex) {
                LOG.error("Got exception ", ex);
                throw new FailedException(ex.getCause());
            }
        }
    }

    public void start() {
        this.executorFuture = executorService.scheduleAtFixedRate(this, interval, interval, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        LOG.debug("Shutting down WaterMarkEventGenerator");
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
