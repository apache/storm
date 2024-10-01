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

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalFsBlobStoreFileTest {

    private File tempFile;
    private LocalFsBlobStoreFile blobStoreFile;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile(null, ".tmp").toFile();
        try (FileOutputStream fs = new FileOutputStream(tempFile)) {
            fs.write("Content for SHA hash".getBytes());
        }
        blobStoreFile = new LocalFsBlobStoreFile(tempFile.getParentFile(), tempFile.getName());
    }

    @Test
    void testGetVersion() throws IOException {
        long expectedVersion = Arrays.hashCode(DigestUtils.sha1("Content for SHA hash"));
        long actualVersion = blobStoreFile.getVersion();
        assertEquals(expectedVersion, actualVersion, "The version should match the expected hash code.");
    }

    @Test
    void testGetVersion_Mismatch() throws IOException {
        long expectedVersion = Arrays.hashCode(DigestUtils.sha1("Different content"));
        long actualVersion = blobStoreFile.getVersion();
        assertNotEquals(expectedVersion, actualVersion, "The version shouldn't match the hash code of different content.");
    }

    @Test
    void testGetVersion_FileNotFound() {
        boolean deleted = tempFile.delete();
        if (!deleted) {
            throw new IllegalStateException("Failed to delete the temporary file.");
        }
        assertThrows(IOException.class, () -> blobStoreFile.getVersion(), "Should throw IOException if file is not found.");
    }

    @Test
    void testGetModTime() throws IOException {
        long expectedModTime = tempFile.lastModified();
        long actualModTime = blobStoreFile.getModTime();
        assertEquals(expectedModTime, actualModTime, "The modification time should match the expected value.");
    }
}
