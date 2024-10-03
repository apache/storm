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

package org.apache.storm.blobstore;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class LocalFsBlobStoreFileTest {

    private File tempFile;
    private LocalFsBlobStoreFile blobStoreFile;
    private CRC32C checksumAlgorithm;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile(null, ".tmp").toFile();
        try (FileOutputStream fs = new FileOutputStream(tempFile)) {
            fs.write("Content for checksum".getBytes());
        }
        blobStoreFile = new LocalFsBlobStoreFile(tempFile.getParentFile(), tempFile.getName());
         checksumAlgorithm= new CRC32C();
    }

    @Test
    void testGetVersion() throws IOException {
        long expectedVersion = FileUtils.checksum(tempFile, checksumAlgorithm).getValue();
        long actualVersion = blobStoreFile.getVersion();
        assertEquals(expectedVersion, actualVersion, "The version should match the expected checksum value.");
    }

    @Test
    void testGetVersion_Mismatch() throws IOException {
        long expectedVersion = FileUtils.checksum(tempFile, checksumAlgorithm).getValue();
        try (FileOutputStream fs = new FileOutputStream(tempFile)) {
            fs.write("Different content".getBytes());
        }
        long actualVersion = blobStoreFile.getVersion();
        assertNotEquals(expectedVersion, actualVersion, "The version shouldn't match the checksum value of different content.");
    }

    @Test
    void testGetModTime() throws IOException {
        long expectedModTime = tempFile.lastModified();
        long actualModTime = blobStoreFile.getModTime();
        assertEquals(expectedModTime, actualModTime, "The modification time should match the expected value.");
    }
}
