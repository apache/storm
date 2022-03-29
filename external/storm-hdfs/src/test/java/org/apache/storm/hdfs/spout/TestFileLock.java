/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.storm.hdfs.common.HdfsUtils;
import org.apache.storm.hdfs.testing.MiniDFSClusterRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFileLock {

    private final Path filesDir = new Path("/tmp/filesdir");
    private final Path locksDir = new Path("/tmp/locksdir");
    @Rule
    public MiniDFSClusterRule dfsClusterRule = new MiniDFSClusterRule();
    private FileSystem fs;
    private HdfsConfiguration conf = new HdfsConfiguration();

    public static void closeUnderlyingLockFile(FileLock lock) throws ReflectiveOperationException {
        Method m = FileLock.class.getDeclaredMethod("forceCloseLockFile");
        m.setAccessible(true);
        m.invoke(lock);
    }

    @BeforeEach
    public void setup() throws IOException {
        conf.set(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, "5000");
        fs = dfsClusterRule.getDfscluster().getFileSystem();
        assert fs.mkdirs(filesDir);
        assert fs.mkdirs(locksDir);
    }

    @AfterEach
    public void teardown() throws IOException {
        fs.delete(filesDir, true);
        fs.delete(locksDir, true);
        fs.close();
    }

    @Test
    public void testBasicLocking() throws Exception {
        // create empty files in filesDir
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
        fs.create(file1).close();
        fs.create(file2).close(); // create empty file

        // acquire lock on file1 and verify if worked
        FileLock lock1a = FileLock.tryLock(fs, file1, locksDir, "spout1");
        assertNotNull(lock1a);
        assertTrue(fs.exists(lock1a.getLockFile()));
        assertEquals(lock1a.getLockFile().getParent(), locksDir); // verify lock file location
        assertEquals(lock1a.getLockFile().getName(), file1.getName()); // verify lock filename

        // acquire another lock on file1 and verify it failed
        FileLock lock1b = FileLock.tryLock(fs, file1, locksDir, "spout1");
        assertNull(lock1b);

        // release lock on file1 and check
        lock1a.release();
        assertFalse(fs.exists(lock1a.getLockFile()));

        // Retry locking and verify
        FileLock lock1c = FileLock.tryLock(fs, file1, locksDir, "spout1");
        assertNotNull(lock1c);
        assertTrue(fs.exists(lock1c.getLockFile()));
        assertEquals(lock1c.getLockFile().getParent(), locksDir); // verify lock file location
        assertEquals(lock1c.getLockFile().getName(), file1.getName()); // verify lock filename

        // try locking another file2 at the same time
        FileLock lock2a = FileLock.tryLock(fs, file2, locksDir, "spout1");
        assertNotNull(lock2a);
        assertTrue(fs.exists(lock2a.getLockFile()));
        assertEquals(lock2a.getLockFile().getParent(), locksDir); // verify lock file location
        assertEquals(lock2a.getLockFile().getName(), file2.getName()); // verify lock filename

        // release both locks
        lock2a.release();
        assertFalse(fs.exists(lock2a.getLockFile()));
        lock1c.release();
        assertFalse(fs.exists(lock1c.getLockFile()));
    }

    @Test
    public void testHeartbeat() throws Exception {
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        fs.create(file1).close();

        // acquire lock on file1
        FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
        assertNotNull(lock1);
        assertTrue(fs.exists(lock1.getLockFile()));

        ArrayList<String> lines = readTextFile(lock1.getLockFile());
        assertEquals(1, lines.size(), "heartbeats appear to be missing");

        // heartbeat upon it
        lock1.heartbeat("1");
        lock1.heartbeat("2");
        lock1.heartbeat("3");

        lines = readTextFile(lock1.getLockFile());
        assertEquals(4, lines.size(), "heartbeats appear to be missing");

        lock1.heartbeat("4");
        lock1.heartbeat("5");
        lock1.heartbeat("6");

        lines = readTextFile(lock1.getLockFile());
        assertEquals(7, lines.size(), "heartbeats appear to be missing");

        lock1.release();
        lines = readTextFile(lock1.getLockFile());
        assertNull(lines);
        assertFalse(fs.exists(lock1.getLockFile()));
    }

    @Test
    public void testConcurrentLocking() throws IOException, InterruptedException {
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        fs.create(file1).close();

        FileLockingThread[] threads = null;
        try {
            threads = startThreads(100, file1, locksDir);
            for (FileLockingThread thd : threads) {
                thd.join(30_000);
                assertTrue(thd.cleanExit, thd.getName() + " did not exit cleanly");
            }

            Path lockFile = new Path(locksDir + Path.SEPARATOR + file1.getName());
            assertFalse(fs.exists(lockFile));
        } finally {
            if (threads != null) {
                for (FileLockingThread thread : threads) {
                    thread.interrupt();
                    thread.join(30_000);
                    if (thread.isAlive()) {
                        throw new RuntimeException("Failed to stop threads within 30 seconds, threads may leak into other tests");
                    }
                }
            }
        }
    }

    private FileLockingThread[] startThreads(int thdCount, Path fileToLock, Path locksDir)
        throws IOException {
        FileLockingThread[] result = new FileLockingThread[thdCount];
        for (int i = 0; i < thdCount; i++) {
            result[i] = new FileLockingThread(i, fs, fileToLock, locksDir, "spout" + i);
        }

        for (FileLockingThread thd : result) {
            thd.start();
        }
        return result;
    }

    @Test
    public void testStaleLockDetection_SingleLock() throws Exception {
        final int LOCK_EXPIRY_SEC = 1;
        final int WAIT_MSEC = 1500;
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        fs.create(file1).close();
        FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
        try {
            // acquire lock on file1
            assertNotNull(lock1);
            assertTrue(fs.exists(lock1.getLockFile()));
            Thread.sleep(WAIT_MSEC);   // wait for lock to expire
            HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
            assertNotNull(expired);

            // heartbeat, ensure its no longer stale and read back the heartbeat data
            lock1.heartbeat("1");
            expired = FileLock.locateOldestExpiredLock(fs, locksDir, 1);
            assertNull(expired);

            FileLock.LogEntry lastEntry = lock1.getLastLogEntry();
            assertNotNull(lastEntry);
            assertEquals("1", lastEntry.fileOffset);

            // wait and check for expiry again
            Thread.sleep(WAIT_MSEC);
            expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
            assertNotNull(expired);
        } finally {
            lock1.release();
            fs.delete(file1, false);
        }
    }

    @Test
    public void testStaleLockDetection_MultipleLocks() throws Exception {
        final int LOCK_EXPIRY_SEC = 1;
        final int WAIT_MSEC = 1500;
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
        Path file3 = new Path(filesDir + Path.SEPARATOR + "file3");

        fs.create(file1).close();
        fs.create(file2).close();
        fs.create(file3).close();

        // 1) acquire locks on file1,file2,file3
        FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
        FileLock lock2 = FileLock.tryLock(fs, file2, locksDir, "spout2");
        FileLock lock3 = FileLock.tryLock(fs, file3, locksDir, "spout3");
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotNull(lock3);

        try {
            HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
            failassertNull(expired);

            // 2) wait for all 3 locks to expire then heart beat on 2 locks and verify stale lock
            Thread.sleep(WAIT_MSEC);
            lock1.heartbeat("1");
            lock2.heartbeat("1");

            expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
            assertNotNull(expired);
            assertEquals("spout3", expired.getValue().componentId);
        } finally {
            lock1.release();
            lock2.release();
            lock3.release();
            fs.delete(file1, false);
            fs.delete(file2, false);
            fs.delete(file3, false);
        }
    }

    @Test
    public void testLockRecovery() throws Exception {
        final int LOCK_EXPIRY_SEC = 1;
        final int WAIT_MSEC = LOCK_EXPIRY_SEC * 1000 + 500;
        Path file1 = new Path(filesDir + Path.SEPARATOR + "file1");
        Path file2 = new Path(filesDir + Path.SEPARATOR + "file2");
        Path file3 = new Path(filesDir + Path.SEPARATOR + "file3");

        fs.create(file1).close();
        fs.create(file2).close();
        fs.create(file3).close();

        // 1) acquire locks on file1,file2,file3
        FileLock lock1 = FileLock.tryLock(fs, file1, locksDir, "spout1");
        FileLock lock2 = FileLock.tryLock(fs, file2, locksDir, "spout2");
        FileLock lock3 = FileLock.tryLock(fs, file3, locksDir, "spout3");
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotNull(lock3);

        try {
            HdfsUtils.Pair<Path, FileLock.LogEntry> expired = FileLock.locateOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC);
            assertNull(expired);

            // 1) Simulate lock file lease expiring and getting closed by HDFS
            closeUnderlyingLockFile(lock3);

            // 2) wait for all 3 locks to expire then heart beat on 2 locks
            Thread.sleep(WAIT_MSEC * 2); // wait for locks to expire
            lock1.heartbeat("1");
            lock2.heartbeat("1");

            // 3) Take ownership of stale lock
            FileLock lock3b = FileLock.acquireOldestExpiredLock(fs, locksDir, LOCK_EXPIRY_SEC, "spout1");
            assertNotNull(lock3b);
            assertEquals(Path.getPathWithoutSchemeAndAuthority(lock3b.getLockFile()), lock3.getLockFile(), "Expected lock3 file");
        } finally {
            lock1.release();
            lock2.release();
            lock3.release();
            fs.delete(file1, false);
            fs.delete(file2, false);
            try {
                fs.delete(file3, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * return null if file not found
     */
    private ArrayList<String> readTextFile(Path file) throws IOException {
        try (FSDataInputStream os = fs.open(file)) {
            if (os == null) {
                return null;
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(os));
            ArrayList<String> lines = new ArrayList<>();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                lines.add(line);
            }
            return lines;
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    static class FileLockingThread extends Thread {

        private final FileSystem fs;
        public boolean cleanExit = false;
        private final int thdNum;
        private final Path fileToLock;
        private final Path locksDir;
        private final String spoutId;

        public FileLockingThread(int thdNum, FileSystem fs, Path fileToLock, Path locksDir, String spoutId) {
            this.thdNum = thdNum;
            this.fs = fs;
            this.fileToLock = fileToLock;
            this.locksDir = locksDir;
            this.spoutId = spoutId;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("FileLockingThread-" + thdNum);
            FileLock lock = null;
            try {
                do {
                    System.err.println("Trying lock - " + getName());
                    lock = FileLock.tryLock(fs, this.fileToLock, this.locksDir, spoutId);
                    System.err.println("Acquired lock - " + getName());
                    if (lock == null) {
                        System.out.println("Retrying lock - " + getName());
                    }
                } while (lock == null && !Thread.currentThread().isInterrupted());
                cleanExit = true;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (lock != null) {
                        lock.release();
                        System.err.println("Released lock - " + getName());
                    }
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
            System.err.println("Thread exiting - " + getName());
        } // run()

    } // class FileLockingThread
}
