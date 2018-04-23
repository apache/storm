/**
 * Copyright (c) 2009-2011 VMware, Inc. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.testtools;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.rules.ExternalResource;

/**
 *
 * An in-memory Cassandra storage service.
 * Useful for unit testing
 *
 * This implementation is based on the springsource community project,
 * com.springsource.insight:insight-plugin-cassandra12.
 *
 * {@see <a href="https://github.com/spring-projects/spring-insight-plugins/blob/c2986b457b482cd08a77a26297c087df59535067/collection
 * -plugins/cassandra12/src/test/java/com/springsource/insight/plugin/cassandra/embeded/EmbeddedCassandraService.java">
 *     com.springsource.insight:insight-plugin-cassandra12
 *     </a>}
 *
 * It has been repurposed to a JUnit external resource by:
 * - Extending ExternalResource instead of implementing Runnable.
 * - Exposing the host and port to use for native connections.
 *
 */

public class EmbeddedCassandraResource extends ExternalResource {
    private final String host;
    private final Integer nativeTransportPort;
    CassandraDaemon cassandraDaemon;

    public EmbeddedCassandraResource() {
        try {
            prepare();
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.init(null);
            host = DatabaseDescriptor.getRpcAddress().getHostName();
            nativeTransportPort = DatabaseDescriptor.getNativeTransportPort();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected void before() throws Throwable {
        cassandraDaemon.start();
    }

    @Override
    protected void after() {

        // Cassandra daemon calls System.exit() on windows, which kills the test.
        // Stop services without killing the process instead.
        if (FBUtilities.isWindows()) {
            cassandraDaemon.thriftServer.stop();
            cassandraDaemon.nativeServer.stop();
        } else {
            cassandraDaemon.stop();
        }

        // Register file cleanup after jvm shutdown
        // Cassandra doesn't actually shut down until jvm shutdown so need to wait for that first.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Sleep before cleaning up files
            try {
                Thread.sleep(3000L);
                cleanupDataDirectories();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }));

    }

    /**
     * Creates all data dir if they don't exist and cleans them
     *
     * @throws IOException
     */
    public void prepare() throws IOException {
        // Tell cassandra where the configuration files are. Use the test configuration file.
        System.setProperty("storage-config", "../../test/resources");

        cleanupDataDirectories();
        makeDirsIfNotExist();
    }

    /**
     * Deletes all data from cassandra data directories, including the commit log.
     *
     * @throws IOException in case of permissions error etc.
     */
    public void cleanupDataDirectories() throws IOException {
        for (String s : getDataDirs()) {
            cleanDir(s);
        }
    }

    /**
     * Creates the data directories, if they didn't exist.
     *
     * @throws IOException if directories cannot be created (permissions etc).
     */
    public void makeDirsIfNotExist() throws IOException {
        for (String s : getDataDirs()) {
            mkdir(s);
        }
    }

    /**
     * Collects all data dirs and returns a set of String paths on the file system.
     *
     * @return
     */
    private Set<String> getDataDirs() {
        Set<String> dirs = new HashSet<String>();
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            dirs.add(s);
        }
        dirs.add(DatabaseDescriptor.getCommitLogLocation());
        dirs.add(DatabaseDescriptor.getSavedCachesLocation());
        return dirs;
    }

    /**
     * Creates a directory
     *
     * @param dir
     * @throws IOException
     */
    private void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

    /**
     * Removes all directory content from file the system
     *
     * @param dir
     * @throws IOException
     */
    private void cleanDir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists() && dirFile.isDirectory()) {
            FileUtils.deleteRecursive(dirFile);
        }
    }

    /**
     * Returns the native port of the server.
     * @return the port number.
     */
    public Integer getNativeTransportPort() {
        return nativeTransportPort;
    }

    /**
     * Returns the host name of the server.
     * @return the host name (typically 127.0.0.1).
     */
    public String getHost() {
        return host;
    }
}
