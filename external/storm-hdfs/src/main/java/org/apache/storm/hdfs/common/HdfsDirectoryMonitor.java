/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.common;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HdfsDirectoryMonitor implements Iterable<Path> {
    public enum WatchMode { POLL, INOTIFY }

    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

    private Path watchDir = null;
    private WatchMode watchMode = null;
    private FileSystem hdfs = null;
    private DFSClient dfsClient = null;
    private HdfsAdmin hdfsAdmin = null;

    private ArrayList<Path> newFiles = new ArrayList<>();
    private long lastTxId = -1;

    private HdfsDirectoryMonitor(Path watchDir, WatchMode watchMode, FileSystem hdfs, DFSClient dfsClient,
                                 HdfsAdmin hdfsAdmin) {
        this.watchDir = watchDir;
        this.watchMode = watchMode;
        this.hdfs = hdfs;
        this.dfsClient = dfsClient;
        this.hdfsAdmin = hdfsAdmin;
    }

    public static HdfsDirectoryMonitor createPollMonitor(Path watchDir, FileSystem hdfs) {
        LOG.info("Instantiating Poll based HDFS Directory Monitor");
        return new HdfsDirectoryMonitor(watchDir, WatchMode.POLL, hdfs, null, null);
    }

    public static HdfsDirectoryMonitor createInotifyMonitor(Path watchDir, FileSystem hdfs, DFSClient dfsClient,
                                                            HdfsAdmin hdfsAdmin) throws IOException {
        LOG.info("Instantiating inotify based HDFS Directory Monitor");

        /* Initialize the last Tx Id to catch the inotify events after the monitor has been created */
        HdfsDirectoryMonitor monitor = new HdfsDirectoryMonitor(watchDir, WatchMode.INOTIFY, hdfs, dfsClient, hdfsAdmin);
        monitor.lastTxId = dfsClient.getNamenode().getCurrentEditLogTxid();

        return monitor;
    }


    public void update() throws IOException, MissingEventsException, InterruptedException {
        /* The poll method lists the directory contents while inotify uses incremental updates.
           Therefore the new file list needs to be replaced in case of poll but added to existing in case of inotify.
         */
        if (this.watchMode == WatchMode.POLL) {
            newFiles = HdfsUtils.listFilesByModificationTime(hdfs, watchDir, 0);
        } else {
            newFiles.addAll(getNewFilesFromInotify(watchDir.toString()));
        }
    }

    private boolean filterFiles(String watchDirectory, String filename)
    {
        /* include files which are located inside the watch directory */
        return !filename.equals(watchDirectory) &&
                filename.startsWith(watchDirectory.endsWith("/") ? watchDirectory : watchDirectory + "/");
    }

    private ArrayList<Path> getNewFilesFromInotify(String watchDirectory)
            throws IOException, InterruptedException, MissingEventsException {

        if (lastTxId == -1) {
            lastTxId = dfsClient.getNamenode().getCurrentEditLogTxid();
        }

        LOG.trace("Reading inotify events from HDFS from tx id: " + lastTxId);

        DFSInotifyEventInputStream eventStream = hdfsAdmin.getInotifyEventStream(this.lastTxId);

        EventBatch batch;
        Set<Path> modifiedFiles = new HashSet<>();
        while ( (batch = eventStream.poll()) != null) {
            for (Event event : batch.getEvents()) {
                switch (event.getEventType()) {
                    case CREATE: {
                        /* inotify event's getPath() returns a string, not a real Path object.
                           We need to construct the full Path object using hdfs.makeQualified()
                           to get the same granularity (scheme + authority + path) as created
                           by HdfsUtils.listFilesByModificationTime */
                        String filename = ((Event.CreateEvent) event).getPath();

                        if (filterFiles(watchDirectory, filename)) {
                            Path path = hdfs.makeQualified(new Path(filename));
                            modifiedFiles.add(path);
                        }
                    }
                    break;

                    case UNLINK: {
                        String filename = ((Event.UnlinkEvent) event).getPath();

                        if (filterFiles(watchDirectory, filename)) {
                            Path path = hdfs.makeQualified(new Path(filename));
                            modifiedFiles.add(path);
                        }
                    }
                    break;

                    case RENAME: {
                        String srcFilename = ((Event.RenameEvent) event).getSrcPath();
                        String dstFilename = ((Event.RenameEvent) event).getDstPath();

                        if (filterFiles(watchDirectory, srcFilename) && filterFiles(watchDirectory, dstFilename)) {
                            Path srcPath = hdfs.makeQualified(new Path(srcFilename));
                            Path dstPath = hdfs.makeQualified(new Path(dstFilename));

                            modifiedFiles.remove(srcPath);
                            modifiedFiles.add(dstPath);
                        }
                    }
                    break;
                }
            }

            lastTxId = batch.getTxid();
        }

        return new ArrayList<>(modifiedFiles);
    }

    @Override
    public Iterator<Path> iterator() {
        return new HdfsDirectoryMonitorIterator();
    }

    private class HdfsDirectoryMonitorIterator implements Iterator<Path> {
        HdfsDirectoryMonitorIterator() {
        }

        @Override
        public boolean hasNext() {
            return newFiles.size() > 0;
        }

        @Override
        public Path next() {
            if (newFiles == null || newFiles.size() == 0) {
                throw new NoSuchElementException();
            }
            return newFiles.remove(0);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Delete Not Supported");
        }
    }
}
