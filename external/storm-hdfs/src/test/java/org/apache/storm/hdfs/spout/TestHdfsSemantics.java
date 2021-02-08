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

import static org.hamcrest.core.IsNull.notNullValue;

import java.io.IOException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.storm.hdfs.testing.MiniDFSClusterRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestHdfsSemantics {

    private final HdfsConfiguration conf = new HdfsConfiguration();
    private final Path dir = new Path("/tmp/filesdir");
    @Rule
    public MiniDFSClusterRule dfsClusterRule = new MiniDFSClusterRule();
    private FileSystem fs;

    @Before
    public void setup() throws IOException {
        conf.set(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, "5000");
        fs = dfsClusterRule.getDfscluster().getFileSystem();
        assert fs.mkdirs(dir);
    }

    @After
    public void teardown() throws IOException {
        fs.delete(dir, true);
        fs.close();
    }

    @Test
    public void testDeleteSemantics() throws Exception {
        Path file = new Path(dir.toString() + Path.SEPARATOR_CHAR + "file1");
        //    try {
        // 1) Delete absent file - should return false
        Assert.assertFalse(fs.exists(file));
        try {
            Assert.assertFalse(fs.delete(file, false));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2) deleting open file - should return true
        fs.create(file, false);
        Assert.assertTrue(fs.delete(file, false));

        // 3) deleting closed file  - should return true
        FSDataOutputStream os = fs.create(file, false);
        os.close();
        Assert.assertTrue(fs.exists(file));
        Assert.assertTrue(fs.delete(file, false));
        Assert.assertFalse(fs.exists(file));
    }

    @Test
    public void testConcurrentDeletion() throws Exception {
        Path file = new Path(dir.toString() + Path.SEPARATOR_CHAR + "file1");
        fs.create(file).close();
        // 1 concurrent deletion - only one thread should succeed
        FileDeletionThread[] threads = null;
        try {
            threads = startThreads(10, file);
            int successCount = 0;
            for (FileDeletionThread thd : threads) {
                thd.join(30_000);
                if (thd.succeeded) {
                    successCount++;
                }
                if (thd.exception != null) {
                    Assert.assertNotNull(thd.exception);
                }
            }
            System.err.println(successCount);
            Assert.assertEquals(1, successCount);
        } finally {
            if (threads != null) {
                for (FileDeletionThread thread : threads) {
                    thread.interrupt();
                    thread.join(30_000);
                    if (thread.isAlive()) {
                        throw new RuntimeException("Failed to stop threads within 30 seconds, threads may leak into other tests");
                    }
                }
            }
        }
    }

    @Test
    public void testAppendSemantics() throws Exception {
        //1 try to append to an open file
        Path file1 = new Path(dir.toString() + Path.SEPARATOR_CHAR + "file1");
        try (FSDataOutputStream os1 = fs.create(file1, false)) {
            fs.append(file1); // should fail
            fail("Append did not throw an exception");
        } catch (RemoteException e) {
            // expecting AlreadyBeingCreatedException inside RemoteException
            Assert.assertEquals(AlreadyBeingCreatedException.class, e.unwrapRemoteException().getClass());
        }

        //2 try to append to a closed file
        try (FSDataOutputStream os2 = fs.append(file1)) {
            assertThat(os2, notNullValue());
        }
    }

    @Test
    public void testDoubleCreateSemantics() throws Exception {
        //1 create an already existing open file w/o override flag
        Path file1 = new Path(dir.toString() + Path.SEPARATOR_CHAR + "file1");
        try (FSDataOutputStream os1 = fs.create(file1, false)) {
            fs.create(file1, false); // should fail
            fail("Create did not throw an exception");
        } catch (RemoteException e) {
            Assert.assertEquals(AlreadyBeingCreatedException.class, e.unwrapRemoteException().getClass());
        }
        //2 close file and retry creation
        try {
            fs.create(file1, false);  // should still fail
            fail("Create did not throw an exception");
        } catch (FileAlreadyExistsException e) {
            // expecting this exception
        }

        //3 delete file and retry creation
        fs.delete(file1, false);
        try (FSDataOutputStream os2 = fs.create(file1, false)) {
            Assert.assertNotNull(os2);
        }
    }

    private FileDeletionThread[] startThreads(int thdCount, Path file)
        throws IOException {
        FileDeletionThread[] result = new FileDeletionThread[thdCount];
        for (int i = 0; i < thdCount; i++) {
            result[i] = new FileDeletionThread(i, fs, file);
        }

        for (FileDeletionThread thd : result) {
            thd.start();
        }
        return result;
    }

    private static class FileDeletionThread extends Thread {

        private final int thdNum;
        private final FileSystem fs;
        private final Path file;
        public boolean succeeded;
        public Exception exception = null;

        public FileDeletionThread(int thdNum, FileSystem fs, Path file)
            throws IOException {
            this.thdNum = thdNum;
            this.fs = fs;
            this.file = file;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("FileDeletionThread-" + thdNum);
            try {
                succeeded = fs.delete(file, false);
            } catch (Exception e) {
                exception = e;
            }
        } // run()

    } // class FileLockingThread
}
