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

package org.apache.storm.daemon.common;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWatcher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(FileWatcher.class);

    private final WatchService watcher;
    private volatile boolean stopped = false;
    private final Path watchedFile;
    private final Callback callback;
    List<WatchEvent.Kind<Path>> kinds;

    public FileWatcher(final Path watchedFile, Callback callback) throws IOException {
        this(watchedFile, callback, Collections.singletonList(ENTRY_MODIFY));
    }

    public FileWatcher(final Path watchedFile, Callback callback, List<WatchEvent.Kind<Path>> kinds) throws IOException {
        this.watchedFile = watchedFile;
        this.callback = callback;
        Path parent = watchedFile.getParent();
        this.watcher = parent.getFileSystem().newWatchService();
        this.kinds = kinds;
        parent.register(watcher, this.kinds.toArray(new WatchEvent.Kind[0]));
    }

    public void start() {
        Thread t = new Thread(this, "FileWatcher-" + watchedFile.getFileName());
        t.setDaemon(true);
        LOG.info("Starting FileWatcher on {}", watchedFile);
        t.start();
    }

    public void stop() {
        LOG.info("Stopping FileWatcher on {}", watchedFile);
        this.stopped = true;
    }

    @Override
    public void run() {
        while (!stopped) {
            WatchKey watchKey;
            try {
                watchKey = watcher.take();
            } catch (InterruptedException ex) {
                LOG.warn("FileWatch for {} is interrupted", watchedFile, ex);
                Thread.currentThread().interrupt();
                return;
            }
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                if (this.kinds.contains(event.kind()) && event.context().equals(watchedFile.getFileName())) {
                    try {
                        LOG.info("Event {} on {}; invoking callback", event.kind(), watchedFile);
                        callback.run();
                    } catch (Exception ex) {
                        LOG.error("Error invoking FileWatcher callback for {}", watchedFile, ex);
                    }
                }
            }
            watchKey.reset();
        }
    }

    public interface Callback {
        void run() throws Exception;
    }
}
