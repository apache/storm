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

package org.apache.storm.daemon.supervisor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Map;

public interface IAdvancedFSOps {

    /**
     * Set directory permissions to (OWNER)RWX (GROUP)R-X (OTHER)--- On some systems that do not support this, it may become a noop
     *
     * @param dir the directory to change permissions on
     * @throws IOException on any error
     */
    void restrictDirectoryPermissions(Path dir) throws IOException;

    /**
     * Move fromDir to toDir, and try to make it an atomic move if possible
     *
     * @param fromDir what to move
     * @param toDir   where to move it from
     * @throws IOException on any error
     */
    void moveDirectoryPreferAtomic(Path fromDir, Path toDir) throws IOException;

    /**
     * Copy a directory
     *
     * @param fromDir from where
     * @param toDir   to where
     * @throws IOException on any error
     */
    void copyDirectory(Path fromDir, Path toDir) throws IOException;

    /**
     * Setup permissions properly for an internal blob store path
     *
     * @param path the path to set the permissions on
     * @param user the user to change the permissions for
     * @throws IOException on any error
     */
    void setupBlobPermissions(Path path, String user) throws IOException;

    /**
     * Delete a file or a directory and all of the children. If it exists.
     *
     * @param path      what to delete
     * @param user      who to delete it as if doing it as someone else is supported
     * @param logPrefix if an external process needs to be launched to delete the object what prefix to include in the logs
     * @throws IOException on any error.
     */
    void deleteIfExists(Path path, String user, String logPrefix) throws IOException;

    /**
     * Delete a file or a directory and all of the children. If it exists.
     *
     * @param path what to delete
     * @throws IOException on any error.
     */
    void deleteIfExists(Path path) throws IOException;

    /**
     * Setup the permissions for the storm code dir
     *
     * @param user the owner of the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    void setupStormCodeDir(String user, Path path) throws IOException;

    /**
     * Setup the permissions for the worker artifacts dirs
     *
     * @param user the owner of the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    void setupWorkerArtifactsDir(String user, Path path) throws IOException;

    /**
     * Sanity check if everything the topology needs is there for it to run.
     *
     * @param conf       the config of the supervisor
     * @param topologyId the ID of the topology
     * @return true if everything is there, else false
     *
     * @throws IOException on any error
     */
    boolean doRequiredTopoFilesExist(Map<String, Object> conf, String topologyId) throws IOException;

    /**
     * Makes a directory, including any necessary but nonexistent parent directories.
     *
     * @param path the directory to create
     * @throws IOException on any error
     */
    void forceMkdir(Path path) throws IOException;

    /**
     * List the contents of a directory.
     *
     * @param dir    the driectory to list the contents of
     * @param filter a filter to decide if it should be included or not
     * @return A stream of directory entries
     *
     * @throws IOException on any error
     */
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException;

    /**
     * List the contents of a directory.
     *
     * @param dir the driectory to list the contents of
     * @return A stream of directory entries
     *
     * @throws IOException on any error
     */
    DirectoryStream<Path> newDirectoryStream(Path dir) throws IOException;

    /**
     * Check if a file exists or not
     *
     * @param path the path to check
     * @return true if it exists else false
     *
     * @throws IOException on any error.
     */
    boolean fileExists(Path path) throws IOException;

    /**
     * Get a writer for the given location
     *
     * @param file the file to write to
     * @return the Writer to use.
     *
     * @throws IOException on any error
     */
    Writer getWriter(Path file) throws IOException;

    /**
     * Get an output stream to write to a given path
     *
     * @param path the path to write to
     * @return an OutputStream for that file
     *
     * @throws IOException on any error
     */
    OutputStream getOutputStream(Path path) throws IOException;

    /**
     * Dump a string to a file
     *
     * @param location where to write to
     * @param data     the data to write
     * @throws IOException on any error
     */
    void dump(Path location, String data) throws IOException;

    /**
     * Read the contents of a file into a String
     *
     * @param location the file to read
     * @return the contents of the file
     *
     * @throws IOException on any error
     */
    String slurpString(Path location) throws IOException;

    /**
     * Read the contents of a file into a byte array.
     *
     * @param location the file to read
     * @return the contents of the file
     *
     * @throws IOException on any error
     */
    byte[] slurp(Path location) throws IOException;

    /**
     * Create a symbolic link pointing at target
     *
     * @param link   the link to create
     * @param target where it should point to
     * @throws IOException on any error.
     */
    void createSymlink(Path link, Path target) throws IOException;
}
