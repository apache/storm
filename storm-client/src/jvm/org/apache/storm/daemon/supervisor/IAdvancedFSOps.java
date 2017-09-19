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


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Map;

public interface IAdvancedFSOps {

    /**
     * Set directory permissions to (OWNER)RWX (GROUP)R-X (OTHER)---
     * On some systems that do not support this, it may become a noop
     * @param dir the directory to change permissions on
     * @throws IOException on any error
     */
    void restrictDirectoryPermissions(File dir) throws IOException;

    /**
     * Move fromDir to toDir, and try to make it an atomic move if possible
     * @param fromDir what to move
     * @param toDir where to move it from
     * @throws IOException on any error
     */
    void moveDirectoryPreferAtomic(File fromDir, File toDir) throws IOException;

    /**
     * @return true if an atomic directory move works, else false.
     */
    boolean supportsAtomicDirectoryMove();

    /**
     * Copy a directory
     * @param fromDir from where
     * @param toDir to where
     * @throws IOException on any error
     */
    void copyDirectory(File fromDir, File toDir) throws IOException;

    /**
     * Setup permissions properly for an internal blob store path
     * @param path the path to set the permissions on
     * @param user the user to change the permissions for
     * @throws IOException on any error
     */
    void setupBlobPermissions(File path, String user) throws IOException;

    /**
     * Delete a file or a directory and all of the children. If it exists.
     * @param path what to delete
     * @param user who to delete it as if doing it as someone else is supported
     * @param logPrefix if an external process needs to be launched to delete
     * the object what prefix to include in the logs
     * @throws IOException on any error.
     */
    void deleteIfExists(File path, String user, String logPrefix) throws IOException;

    /**
     * Delete a file or a directory and all of the children. If it exists.
     * @param path what to delete
     * @throws IOException on any error.
     */
    void deleteIfExists(File path) throws IOException;

    /**
     * Setup the permissions for the storm code dir
     * @param user the owner of the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    void setupStormCodeDir(String user, File path) throws IOException;

    /**
     * Setup the permissions for the worker artifacts dirs
     * @param user the owner of the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    void setupWorkerArtifactsDir(String user, File path) throws IOException;

    /**
     * Sanity check if everything the topology needs is there for it to run.
     * @param conf the config of the supervisor
     * @param topologyId the ID of the topology
     * @return true if everything is there, else false
     * @throws IOException on any error
     */
    boolean doRequiredTopoFilesExist(Map<String, Object> conf, String topologyId) throws IOException;

    /**
     * Makes a directory, including any necessary but nonexistent parent
     * directories.
     *
     * @param path the directory to create
     * @throws IOException on any error
     */
    void forceMkdir(File path) throws IOException;

    /**
     * Check if a file exists or not
     * @param path the path to check
     * @return true if it exists else false
     * @throws IOException on any error.
     */
    boolean fileExists(File path) throws IOException;

    /**
     * Get a writer for the given location
     * @param file the file to write to
     * @return the Writer to use.
     * @throws IOException on any error
     */
    Writer getWriter(File file) throws IOException;

    /**
     * Get an output stream to write to a given file
     * @param file the file to write to
     * @return an OutputStream for that file
     * @throws IOException on any error
     */
    OutputStream getOutputStream(File file) throws IOException;

    /**
     * Dump a string to a file
     * @param location where to write to
     * @param data the data to write
     * @throws IOException on any error
     */
    void dump(File location, String data) throws IOException;

    /**
     * Read the contents of a file into a String
     * @param location the file to read
     * @return the contents of the file
     * @throws IOException on any error
     */
    String slurpString(File location) throws IOException;

    /**
     * Read the contents of a file into a byte array.
     * @param location the file to read
     * @return the contents of the file
     * @throws IOException on any error
     */
    byte[] slurp(File location) throws IOException;

    /**
     * Create a symbolic link pointing at target
     * @param link the link to create
     * @param target where it should point to
     * @throws IOException on any error.
     */
    void createSymlink(File link, File target) throws IOException;
}
