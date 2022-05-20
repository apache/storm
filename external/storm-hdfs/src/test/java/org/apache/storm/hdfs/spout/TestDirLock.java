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

import java.io.IOException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.storm.hdfs.testing.MiniDFSClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirLock {

    private static final int LOCK_EXPIRY_SEC = 1;
    private final Path locksDir = new Path("/tmp/lockdir");
    @RegisterExtension
    public static final MiniDFSClusterExtension DFS_CLUSTER_EXTENSION = new MiniDFSClusterExtension();
    private FileSystem fs;
    private HdfsConfiguration conf = new HdfsConfiguration();

    @BeforeEach
    public void setUp() throws IOException {
        conf.set(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, "5000");
        fs = DFS_CLUSTER_EXTENSION.getDfscluster().getFileSystem();
        assert fs.mkdirs(locksDir);
    }

    @AfterEach
    public void teardownClass() throws IOException {
        fs.delete(locksDir, true);
        fs.close();
    }

    @Test
    public void testBasicLocking() throws Exception {
        // 1 grab lock
        DirLock lock = DirLock.tryLock(fs, locksDir);
        assertTrue(fs.exists(lock.getLockFile()));

        // 2 try to grab another lock while dir is locked
        DirLock lock2 = DirLock.tryLock(fs, locksDir); // should fail
        assertNull(lock2);

        // 3 let go first lock
        lock.release();
        assertFalse(fs.exists(lock.getLockFile()));

        // 4 try locking again
        lock2 = DirLock.tryLock(fs, locksDir);
        assertTrue(fs.exists(lock2.getLockFile()));
        lock2.release();
        assertFalse(fs.exists(lock.getLockFile()));
        lock2.release();  // should be thrown
    }

    @Test
    public void testConcurrentLocking() throws Exception {
        DirLockingThread[] threads = null;
        try {
            threads = startThreads(100, locksDir);
            for (DirLockingThread thd : threads) {
                thd.join(30_000);
                assertTrue(thd.cleanExit, thd.getName() + " did not exit cleanly");
            }

            Path lockFile = new Path(locksDir + Path.SEPARATOR + DirLock.DIR_LOCK_FILE);
            assertFalse(fs.exists(lockFile));
        } finally {
            if (threads != null) {
                for (DirLockingThread thread : threads) {
                    thread.interrupt();
                    thread.join(30_000);
                    if (thread.isAlive()) {
                        throw new RuntimeException("Failed to stop threads within 30 seconds, threads may leak into other tests");
                    }
                }
            }
        }
    }

    private DirLockingThread[] startThreads(int thdCount, Path dir)
        throws IOException {
        DirLockingThread[] result = new DirLockingThread[thdCount];
        for (int i = 0; i < thdCount; i++) {
            result[i] = new DirLockingThread(i, fs, dir);
        }

        for (DirLockingThread thd : result) {
            thd.start();
        }
        return result;
    }

    @Test
    public void testLockRecovery() throws Exception {
        DirLock lock1 = DirLock.tryLock(fs, locksDir);   // should pass
        assertNotNull(lock1);

        DirLock lock2 = DirLock.takeOwnershipIfStale(fs, locksDir, LOCK_EXPIRY_SEC); // should fail
        assertNull(lock2);

        Thread.sleep(LOCK_EXPIRY_SEC * 1000 + 500); // wait for lock to expire
        assertTrue(fs.exists(lock1.getLockFile()));

        DirLock lock3 = DirLock.takeOwnershipIfStale(fs, locksDir, LOCK_EXPIRY_SEC); // should pass now
        assertNotNull(lock3);
        assertTrue(fs.exists(lock3.getLockFile()));
        lock3.release();
        assertFalse(fs.exists(lock3.getLockFile()));
        lock1.release(); // should not throw
    }

    static class DirLockingThread extends Thread {

        private final FileSystem fs;
        private final Path dir;
        public boolean cleanExit = false;
        private int thdNum;

        public DirLockingThread(int thdNum, FileSystem fs, Path dir) {
            this.thdNum = thdNum;
            this.fs = fs;
            this.dir = dir;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("DirLockingThread-" + thdNum);
            DirLock lock = null;
            try {
                do {
                    System.err.println("Trying lock " + getName());
                    lock = DirLock.tryLock(fs, dir);
                    System.err.println("Acquired lock " + getName());
                    if (lock == null) {
                        System.out.println("Retrying lock - " + getName());
                    }
                } while (lock == null && !Thread.currentThread().isInterrupted());
                cleanExit = true;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (lock != null) {
                        lock.release();
                        System.err.println("Released lock " + getName());
                    }
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
            System.err.println("Thread exiting " + getName());
        } // run()

    } // class DirLockingThread
}
