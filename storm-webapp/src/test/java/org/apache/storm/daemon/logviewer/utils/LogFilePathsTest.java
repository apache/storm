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

package org.apache.storm.daemon.logviewer.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LogFilePathsTest {

    @TempDir
    Path tempDir;

    private Path createWorkerLog(Path root, String topology, String content) throws IOException {
        Path portDir = Files.createDirectories(root.resolve(topology).resolve("6700"));
        return Files.write(portDir.resolve("worker.log"), content.getBytes(StandardCharsets.UTF_8));
    }

    private static Path normalized(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private static String readAll(SeekableByteChannel channel) throws IOException {
        return new String(Channels.newInputStream(channel).readAllBytes(), StandardCharsets.UTF_8);
    }

    @Test
    public void testRegularFileIsAccepted() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path file = normalized(createWorkerLog(root, "topoA", "content"));

        assertTrue(LogFilePaths.isRegularFileWithoutSymbolicLink(root, file));
        assertTrue(LogFilePaths.isServableWorkerFile(root, file));
        assertTrue(LogFilePaths.isRegularFileNotLink(file));
        try (SeekableByteChannel channel = LogFilePaths.openForReading(file, false)) {
            assertEquals("content", readAll(channel));
        }
    }

    @Test
    public void testMissingFileIsRejected() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        createWorkerLog(root, "topoA", "content");

        assertFalse(LogFilePaths.isServableWorkerFile(root, root.resolve("topoA").resolve("6700").resolve("missing.log")));
        assertFalse(LogFilePaths.isServableWorkerFile(root, root.resolve("topoB").resolve("6700").resolve("worker.log")));
    }

    @Test
    public void testDirectoryIsRejected() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path dir = Files.createDirectories(root.resolve("topoA").resolve("6700").resolve("worker.log"));

        assertFalse(LogFilePaths.isServableWorkerFile(root, dir));
    }

    @Test
    public void testFileOutsideRootIsRejected() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path outside = Files.write(tempDir.resolve("outside.txt"), "outside".getBytes(StandardCharsets.UTF_8));

        assertFalse(LogFilePaths.isRegularFileWithoutSymbolicLink(root, normalized(outside)));
        assertFalse(LogFilePaths.isServableWorkerFile(root, normalized(outside)));
    }

    @Test
    public void testOnlyDirectChildrenOfPortDirAreWorkerFiles() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path nested = root.resolve("topoA").resolve("6700").resolve("sub").resolve("worker.log");
        Files.createDirectories(nested.getParent());
        Files.write(nested, "nested".getBytes(StandardCharsets.UTF_8));
        Path shallow = Files.write(root.resolve("topoA").resolve("worker.log"), "shallow".getBytes(StandardCharsets.UTF_8));

        assertTrue(LogFilePaths.isWorkerFilePath(root, root.resolve("topoA").resolve("6700").resolve("worker.log")));
        assertFalse(LogFilePaths.isWorkerFilePath(root, nested));
        assertFalse(LogFilePaths.isServableWorkerFile(root, nested));
        assertFalse(LogFilePaths.isServableWorkerFile(root, shallow));
    }

    @Test
    public void testSymbolicLinkBelowRootIsRejected() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path target = createWorkerLog(root, "topoB", "topoB content");
        Path portDir = Files.createDirectories(root.resolve("topoA").resolve("6700"));
        Path link = Files.createSymbolicLink(portDir.resolve("worker.log"), target);
        Path outside = Files.write(tempDir.resolve("outside.txt"), "outside".getBytes(StandardCharsets.UTF_8));
        Path outsideLink = Files.createSymbolicLink(portDir.resolve("outside.log"), outside);

        assertFalse(LogFilePaths.isServableWorkerFile(root, normalized(link)));
        assertFalse(LogFilePaths.isServableWorkerFile(root, normalized(outsideLink)));
        assertFalse(LogFilePaths.isRegularFileNotLink(link));
        assertNull(LogFilePaths.openForReading(link, false));
        try (SeekableByteChannel channel = LogFilePaths.openForReading(link, true)) {
            assertEquals("topoB content", readAll(channel));
        }
    }

    @Test
    public void testOpenReportsFileReplacedBySymbolicLinkAfterCheck() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path file = createWorkerLog(root, "topoA", "topoA content");
        Path target = createWorkerLog(root, "topoB", "topoB content");
        assertTrue(LogFilePaths.isServableWorkerFile(root, normalized(file)));

        //The file is replaced by a symbolic link between the check and the open
        Files.delete(file);
        Files.createSymbolicLink(file, target);

        assertNull(LogFilePaths.openForReading(file, false));
    }

    @Test
    public void testOpenOfMissingFileReturnsNull() throws IOException {
        Path missing = tempDir.resolve("missing.log");

        assertNull(LogFilePaths.openForReading(missing, false));
        assertNull(LogFilePaths.openForReading(missing, true));
    }

    @Test
    public void testOpenReportsFileRemovedAfterCheck() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        Path file = createWorkerLog(root, "topoA", "topoA content");
        assertTrue(LogFilePaths.isServableWorkerFile(root, normalized(file)));

        //The file is removed (e.g. by the log cleaner) between the check and the open
        Files.delete(file);

        assertNull(LogFilePaths.openForReading(file, false));
    }

    @Test
    public void testOpenOfDanglingSymbolicLinkReturnsNull() throws IOException {
        Path link = Files.createSymbolicLink(tempDir.resolve("nimbus.log"), tempDir.resolve("missing.log"));

        assertNull(LogFilePaths.openForReading(link, true));
        assertNull(LogFilePaths.openForReading(link, false));
    }

    @Test
    public void testOpenOfUnreadableFileFails() throws IOException {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        assumeFalse("root".equals(System.getProperty("user.name")));
        Path file = Files.writeString(tempDir.resolve("unreadable.log"), "content");
        Files.setPosixFilePermissions(file, Collections.emptySet());

        //Errors other than a missing file or a symbolic link are still thrown
        assertThrows(AccessDeniedException.class, () -> LogFilePaths.openForReading(file, false));
        assertThrows(AccessDeniedException.class, () -> LogFilePaths.openForReading(file, true));
    }

    @Test
    public void testSymbolicLinkDirectoryBelowRootIsRejected() throws IOException {
        Path root = normalized(Files.createDirectories(tempDir.resolve("logs")));
        createWorkerLog(root, "topoB", "topoB content");
        Path topoDir = Files.createDirectories(root.resolve("topoA"));
        Files.createSymbolicLink(topoDir.resolve("6700"), root.resolve("topoB").resolve("6700"));

        assertFalse(LogFilePaths.isServableWorkerFile(root, topoDir.resolve("6700").resolve("worker.log")));
        assertFalse(LogFilePaths.isDirectoryNotLink(topoDir.resolve("6700")));
        assertTrue(LogFilePaths.isDirectoryNotLink(root.resolve("topoB").resolve("6700")));
    }

    @Test
    public void testSymbolicLinkAsRootIsAccepted() throws IOException {
        Path realRoot = Files.createDirectories(tempDir.resolve("real-logs"));
        createWorkerLog(realRoot, "topoA", "content");
        Path linkRoot = normalized(Files.createSymbolicLink(tempDir.resolve("logs"), realRoot));

        Path file = linkRoot.resolve("topoA").resolve("6700").resolve("worker.log");
        assertTrue(LogFilePaths.isServableWorkerFile(linkRoot, file));
        assertTrue(LogFilePaths.isRegularFileNotLink(file));
        try (SeekableByteChannel channel = LogFilePaths.openForReading(file, false)) {
            assertEquals("content", readAll(channel));
        }
    }

    @Test
    public void testFileLengthOfPlainAndCompressedFile() throws IOException {
        Path plain = Files.write(tempDir.resolve("worker.log"), "plain content".getBytes(StandardCharsets.UTF_8));
        Path compressed = tempDir.resolve("worker.log.1.gz");
        byte[] content = "compressed content".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(compressed))) {
            out.write(content);
        }

        try (SeekableByteChannel channel = LogFilePaths.openForReading(plain, false)) {
            assertEquals(13, LogFilePaths.fileLength(channel, false));
        }
        try (SeekableByteChannel channel = LogFilePaths.openForReading(compressed, false)) {
            assertEquals(content.length, LogFilePaths.fileLength(channel, true));
            //The channel is positioned at the start again, so the content can be read afterwards
            try (InputStream in = new GZIPInputStream(Channels.newInputStream(channel))) {
                assertEquals("compressed content", new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
    }
}
