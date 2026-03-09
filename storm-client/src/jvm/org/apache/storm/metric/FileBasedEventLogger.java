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

package org.apache.storm.metric;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedEventLogger implements IEventLogger {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedEventLogger.class);

    private static final int FLUSH_INTERVAL_MILLIS = 1000;
    private static final long BYTES_PER_MB = 1024L * 1024L;
    private static final int DEFAULT_ROTATION_SIZE_MB = 100;
    private static final int DEFAULT_MAX_RETAINED_FILES = 5;

    private Path eventLogPath;
    private BufferedWriter eventLogWriter;
    private ScheduledExecutorService flushScheduler;
    private volatile boolean dirty = false;
    private final Object writeLock = new Object();

    // File rotation configs
    private long maxFileSize;
    private int maxRetainedFiles;
    private long currentFileSize = 0;

    private void initLogWriter(Path logFilePath) {
        try {
            LOG.info("logFilePath {}", logFilePath);
            eventLogPath = logFilePath;

            currentFileSize = Files.exists(eventLogPath) ? Files.size(eventLogPath) : 0L;

            eventLogWriter = Files.newBufferedWriter(eventLogPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.error("Error setting up FileBasedEventLogger.", e);
            throw new RuntimeException(e);
        }
    }

    private void setUpFlushTask() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("event-logger-flush-%d")
                .setDaemon(true)
                .build();

        flushScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (writeLock) {
                        if (dirty && eventLogWriter != null) {
                            eventLogWriter.flush();
                            dirty = false;
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Error flushing " + eventLogPath, ex);
                }
            }
        };

        flushScheduler.scheduleAtFixedRate(runnable, FLUSH_INTERVAL_MILLIS, FLUSH_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void prepare(Map<String, Object> conf, Map<String, Object> arguments, TopologyContext context) {
        String stormId = context.getStormId();
        int port = context.getThisWorkerPort();

        int rotationSizeMb = ObjectReader.getInt(conf.get(Config.TOPOLOGY_EVENTLOGGER_ROTATION_SIZE_MB),
                DEFAULT_ROTATION_SIZE_MB);
        this.maxFileSize = rotationSizeMb * BYTES_PER_MB;
        this.maxRetainedFiles = ObjectReader.getInt(conf.get(Config.TOPOLOGY_EVENTLOGGER_MAX_RETAINED_FILES),
                DEFAULT_MAX_RETAINED_FILES);

        /*
         * Include the topology name & worker port in the file name so that
         * multiple event loggers can log independently.
         */
        String workersArtifactRoot = ConfigUtils.workerArtifactsRoot(conf, stormId, port);

        Path path = Paths.get(workersArtifactRoot, "events.log");
        try {
            Files.createDirectories(path.getParent());
        } catch (IOException e) {
            LOG.error("Failed to create directories for event logger", e);
            throw new RuntimeException(e);
        }
        initLogWriter(path);
        setUpFlushTask();
    }

    @Override
    public void log(EventInfo event) {
        try {
            String logMessage = buildLogMessage(event);
            int writeLength = logMessage.length() + System.lineSeparator().length();

            synchronized (writeLock) {
                if (currentFileSize + writeLength > maxFileSize) {
                    rotateFiles();
                }

                if (eventLogWriter != null) {
                    eventLogWriter.write(logMessage);
                    eventLogWriter.newLine();
                    currentFileSize += writeLength;
                    dirty = true;
                }
            }
        } catch (IOException ex) {
            LOG.error("Error logging event {}", event, ex);
            throw new RuntimeException(ex);
        }
    }

    private void rotateFiles() throws IOException {
        eventLogWriter.close();

        // Delete any files that exceed maxRetainedFiles (e.g. if the config was
        // lowered)
        int i = maxRetainedFiles;
        while (Files.exists(Paths.get(eventLogPath.toString() + "." + i))) {
            Files.delete(Paths.get(eventLogPath.toString() + "." + i));
            i++;
        }

        // Shift existing rotated files
        for (i = maxRetainedFiles - 1; i >= 1; i--) {
            Path src = Paths.get(eventLogPath.toString() + "." + i);
            Path dst = Paths.get(eventLogPath.toString() + "." + (i + 1));
            if (Files.exists(src)) {
                Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        // Rename current events.log
        if (Files.exists(eventLogPath)) {
            Path dst = Paths.get(eventLogPath.toString() + ".1");
            Files.move(eventLogPath, dst, StandardCopyOption.REPLACE_EXISTING);
        }

        // Re-open writers to empty file
        initLogWriter(eventLogPath);
        currentFileSize = 0;
    }

    protected String buildLogMessage(EventInfo event) {
        return event.toString();
    }

    @Override
    public void close() {
        closeFlushScheduler();

        try {
            synchronized (writeLock) {
                if (eventLogWriter != null) {
                    eventLogWriter.close();
                    eventLogWriter = null;
                }
            }
        } catch (IOException ex) {
            LOG.error("Error closing event log.", ex);
        }
    }

    private void closeFlushScheduler() {
        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                if (!flushScheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    flushScheduler.shutdownNow();
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                flushScheduler.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
}
