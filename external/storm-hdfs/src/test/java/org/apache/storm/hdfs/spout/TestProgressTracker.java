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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

public class TestProgressTracker {

    @TempDir
    public File tempFolder;
    public File baseFolder;
    private FileSystem fs;
    private Configuration conf = new Configuration();

    @BeforeEach
    public void setUp() throws Exception {
        fs = FileSystem.getLocal(conf);
    }

    @Test
    public void testBasic() throws Exception {
        ProgressTracker tracker = new ProgressTracker();
        baseFolder = new File(tempFolder, "trackertest");
        baseFolder.mkdir();

        Path file = new Path(baseFolder.toString() + Path.SEPARATOR + "testHeadTrimming.txt");
        createTextFile(file, 10);

        // create reader and do some checks
        TextFileReader reader = new TextFileReader(fs, file, null);
        FileOffset pos0 = tracker.getCommitPosition();
        assertNull(pos0);

        TextFileReader.Offset currOffset = reader.getFileOffset();
        assertNotNull(currOffset);
        assertEquals(0, currOffset.charOffset);

        // read 1st line and ack
        assertNotNull(reader.next());
        TextFileReader.Offset pos1 = reader.getFileOffset();
        tracker.recordAckedOffset(pos1);

        TextFileReader.Offset pos1b = (TextFileReader.Offset) tracker.getCommitPosition();
        assertEquals(pos1, pos1b);

        // read 2nd line and ACK
        assertNotNull(reader.next());
        TextFileReader.Offset pos2 = reader.getFileOffset();
        tracker.recordAckedOffset(pos2);

        tracker.dumpState(System.err);
        TextFileReader.Offset pos2b = (TextFileReader.Offset) tracker.getCommitPosition();
        assertEquals(pos2, pos2b);

        // read lines 3..7, don't ACK .. commit pos should remain same
        assertNotNull(reader.next());//3
        TextFileReader.Offset pos3 = reader.getFileOffset();
        assertNotNull(reader.next());//4
        TextFileReader.Offset pos4 = reader.getFileOffset();
        assertNotNull(reader.next());//5
        TextFileReader.Offset pos5 = reader.getFileOffset();
        assertNotNull(reader.next());//6
        TextFileReader.Offset pos6 = reader.getFileOffset();
        assertNotNull(reader.next());//7
        TextFileReader.Offset pos7 = reader.getFileOffset();

        // now ack msg 5 and check
        tracker.recordAckedOffset(pos5);
        assertEquals(pos2, tracker.getCommitPosition()); // should remain unchanged @ 2
        tracker.recordAckedOffset(pos4);
        assertEquals(pos2, tracker.getCommitPosition()); // should remain unchanged @ 2
        tracker.recordAckedOffset(pos3);
        assertEquals(pos5, tracker.getCommitPosition()); // should be at 5

        tracker.recordAckedOffset(pos6);
        assertEquals(pos6, tracker.getCommitPosition()); // should be at 6
        tracker.recordAckedOffset(pos6);                        // double ack on same msg
        assertEquals(pos6, tracker.getCommitPosition()); // should still be at 6

        tracker.recordAckedOffset(pos7);
        assertEquals(pos7, tracker.getCommitPosition()); // should be at 7

        tracker.dumpState(System.err);
    }

    private void createTextFile(Path file, int lineCount) throws IOException {
        try (FSDataOutputStream os = fs.create(file)) {
            for (int i = 0; i < lineCount; i++) {
                os.writeBytes("line " + i + System.lineSeparator());
            }
        }
    }

}
