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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.storm.cassandra.trident.MapStateTest.cassandra;

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

public class EmbeddedCassandraResource implements BeforeAllCallback, AfterAllCallback {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCassandraResource.class);
    private final int timeout;
    private String host;
    private Integer nativeTransportPort;
    private CassandraDaemon cassandraDaemon;
    private CqlSession session;


    public EmbeddedCassandraResource(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public void beforeAll(ExtensionContext arg0) {
        if (cassandraDaemon == null) {
            final CountDownLatch startupLatch = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(() -> {
                try {
                    DatabaseDescriptor.daemonInitialization();
                    prepare();
                    cassandraDaemon = new CassandraDaemon(true);
                    cassandraDaemon.activate();
                    host = DatabaseDescriptor.getRpcAddress().getHostName();
                    nativeTransportPort = DatabaseDescriptor.getNativeTransportPort();
                    startupLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
            try {
                if (!startupLatch.await(timeout, MILLISECONDS)) {
                    LOG.error("Cassandra daemon did not start after " + timeout + " ms. Consider increasing the timeout");
                    throw new AssertionError("Cassandra daemon did not start within timeout");
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted waiting for Cassandra daemon to start:", e);
                throw new AssertionError(e);
            } finally {
                executor.shutdown();
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext arg0) {

        // Cassandra daemon calls System.exit() on windows, which kills the test.
        // Stop services without killing the process instead.
        cassandraDaemon.deactivate();

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
            File dir = new File(s);
            if (dir.exists()) {
                LOG.info("Skipping existing directory {}", dir.getCanonicalPath());
            } else {
                LOG.info("Creating missing directory {}", dir.getCanonicalPath());
                mkdir(s);
            }
        }
    }

    /**
     * Collects all data dirs and returns a set of String paths on the file system.
     *
     * @return
     */
    private Set<String> getDataDirs() {
        Set<String> dirs = new HashSet<>();
        try {
            if (DatabaseDescriptor.getAllDataFileLocations() != null) {
                for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
                    if (s != null) {
                        dirs.add(s);
                    }
                }
            }
        } catch (Throwable ex) {
            LOG.error("Cannot extract all DatabaseDescriptor.getAllDataFileLocations()", ex);
            ex.printStackTrace();
        }
        dirs.add(DatabaseDescriptor.getHintsDirectory().absolutePath());
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
    private void mkdir(String dir) {
        new File(dir).mkdirs();
    }

    /**
     * Removes all directory content from file the system
     *
     * @param dir
     * @throws IOException
     */
    private void cleanDir(String dir) {
        Path directory = new File(dir).toPath();
        try {
            Files.walk(directory)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            System.err.println("Error deleting directory: " + e.getMessage());
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

    public CqlSession getSession() {
        initSession();
        return session;
    }

    private synchronized void initSession() {
        if (session == null) {
            CqlSessionBuilder cqlSessionBuilder =
                    CqlSession.builder()
                            .withLocalDatacenter("datacenter1")
                            .addContactPoint(new InetSocketAddress(getHost(), getNativeTransportPort())); //will use autodiscovery, see https://docs.datastax.com/en/developer/java-driver/4.2/manual/core/load_balancing/
            session = cqlSessionBuilder.build();
        }
    }
}
