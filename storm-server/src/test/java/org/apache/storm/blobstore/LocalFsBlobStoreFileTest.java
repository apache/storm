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
