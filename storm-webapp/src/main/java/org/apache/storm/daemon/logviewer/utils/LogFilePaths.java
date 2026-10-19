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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Helpers to serve only files that resolve inside the worker log root.
 *
 * <p>Files below a worker's port directory are created by the worker itself, so a symbolic link placed there could point to the
 * logs of another topology or to any file outside the log root. These helpers make sure such a link is treated like a missing file.
 * The log root itself may still be a symbolic link (or lie below one).
 *
 * <p>Worker files are limited to direct children of a port directory ("topologyId/port/fileName"). The topology and port
 * directories are created by the supervisor, so only the last path element can change between the check and the open, and the
 * open does not follow a symbolic link at the last path element.
 */
public final class LogFilePaths {

    private static final int GZIP_TRAILER_SIZE_BYTES = 4;

    private LogFilePaths() {
    }

    /**
     * Check that a file is a worker file directly below a port directory, i.e. "topologyId/port/fileName" below the log root.
     *
     * @param logRoot the absolute, normalized worker log root
     * @param file the absolute, normalized file path
     * @return true if the file has exactly three path elements below the log root
     */
    public static boolean isWorkerFilePath(Path logRoot, Path file) {
        return file.startsWith(logRoot) && logRoot.relativize(file).getNameCount() == 3;
    }

    /**
     * Check that a file is a worker file ("topologyId/port/fileName") that may be served: a regular file reached without
     * passing through any symbolic link below the log root.
     *
     * @param logRoot the absolute, normalized worker log root; it may itself be or lie below a symbolic link
     * @param file the absolute, normalized file path below logRoot
     * @return true if the file may be served, false otherwise
     */
    public static boolean isServableWorkerFile(Path logRoot, Path file) {
        return isWorkerFilePath(logRoot, file) && isRegularFileWithoutSymbolicLink(logRoot, file);
    }

    /**
     * Check that a file below the given root directory is a regular file and is reached without passing through any symbolic
     * link below the root directory.
     *
     * @param rootDir the absolute, normalized root directory; it may itself be or lie below a symbolic link
     * @param file the absolute, normalized file path below rootDir
     * @return true if the file is a regular file and resolves to exactly the same location below the real root directory,
     *     false otherwise
     */
    public static boolean isRegularFileWithoutSymbolicLink(Path rootDir, Path file) {
        if (!file.startsWith(rootDir) || !isRegularFileNotLink(file)) {
            return false;
        }
        try {
            Path expected = rootDir.toRealPath().resolve(rootDir.relativize(file));
            return file.toRealPath().equals(expected);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Check whether the path is a regular file which is not a symbolic link.
     *
     * @param path the path to check
     * @return true if the path is a regular file and not a symbolic link
     */
    public static boolean isRegularFileNotLink(Path path) {
        return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Check whether the path is a directory which is not a symbolic link.
     *
     * @param path the path to check
     * @return true if the path is a directory and not a symbolic link
     */
    public static boolean isDirectoryNotLink(Path path) {
        return Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Open a file for reading.
     *
     * @param file the file to open
     * @param followLinks whether a symbolic link at the last path element is followed; only the last path element is affected
     * @return a channel for reading the file
     * @throws IOException if the file can not be opened, including when it is a symbolic link and followLinks is false
     */
    public static SeekableByteChannel openForReading(Path file, boolean followLinks) throws IOException {
        return followLinks
            ? Files.newByteChannel(file, StandardOpenOption.READ)
            : Files.newByteChannel(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Get the size of the opened file, or the uncompressed size stored in its gzip trailer. The channel is positioned at the start
     * of the file afterwards.
     *
     * @param channel the opened file
     * @param isZipFile whether the file is gzip compressed
     * @return the (uncompressed) size of the file
     * @throws IOException if the size can not be read
     */
    public static long fileLength(SeekableByteChannel channel, boolean isZipFile) throws IOException {
        long size = channel.size();
        if (!isZipFile) {
            return size;
        }
        if (size < GZIP_TRAILER_SIZE_BYTES) {
            throw new EOFException("File is too short to be gzip compressed");
        }
        ByteBuffer trailer = ByteBuffer.allocate(GZIP_TRAILER_SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(size - GZIP_TRAILER_SIZE_BYTES);
        while (trailer.hasRemaining()) {
            if (channel.read(trailer) < 0) {
                throw new EOFException("Unexpected end of gzip trailer");
            }
        }
        channel.position(0);
        trailer.flip();
        return Integer.toUnsignedLong(trailer.getInt());
    }
}
