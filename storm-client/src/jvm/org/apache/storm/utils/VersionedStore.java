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

package org.apache.storm.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.storm.daemon.supervisor.DirectoryDeleteVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedStore {
    private static final String FINISHED_VERSION_SUFFIX = ".version";
    private static final Logger LOG = LoggerFactory.getLogger(VersionedStore.class);

    private Path _root;

    /**
     * Creates a store at the given path.
     *
     * @param path The path for the store
     * @param createRootDir option to create the path directory
     */
    public VersionedStore(Path path, boolean createRootDir) throws IOException {
        _root = path;
        if (createRootDir) {
            Files.createDirectories(_root);
        }
    }

    public Path getRoot() {
        return _root;
    }

    public Path versionPath(long version) {
        return _root.resolve("" + version).toAbsolutePath();
    }

    public Path mostRecentVersionPath() throws IOException {
        Long v = mostRecentVersion();
        if (v == null) {
            return null;
        }
        return versionPath(v);
    }

    public Path mostRecentVersionPath(long maxVersion) throws IOException {
        Long v = mostRecentVersion(maxVersion);
        if (v == null) {
            return null;
        }
        return versionPath(v);
    }

    public Long mostRecentVersion() throws IOException {
        List<Long> all = getAllVersions();
        if (all.size() == 0) {
            return null;
        }
        return all.get(0);
    }

    public Long mostRecentVersion(long maxVersion) throws IOException {
        List<Long> all = getAllVersions();
        for (Long v : all) {
            if (v <= maxVersion) {
                return v;
            }
        }
        return null;
    }

    public Path createVersion() throws IOException {
        Long mostRecent = mostRecentVersion();
        long version = Time.currentTimeMillis();
        if (mostRecent != null && version <= mostRecent) {
            version = mostRecent + 1;
        }
        return createVersion(version);
    }

    public Path createVersion(long version) throws IOException {
        Path ret = versionPath(version);
        if (getAllVersions().contains(version)) {
            throw new RuntimeException("Version already exists or data already exists");
        } else {
            return ret;
        }
    }

    public void failVersion(Path path) throws IOException {
        deleteVersion(validateAndGetVersion(path));
    }

    public void deleteVersion(long version) throws IOException {
        Path versionFile = versionPath(version);
        Path tokenFile = tokenPath(version);

        if (tokenFile.toFile().exists()) {
            Files.walkFileTree(tokenFile, new DirectoryDeleteVisitor());
        }

        if (versionFile.toFile().exists()) {
            Files.walkFileTree(versionFile, new DirectoryDeleteVisitor());
        }
    }

    public void succeedVersion(Path path) throws IOException {
        long version = validateAndGetVersion(path);
        // should rewrite this to do a file move
        Files.createFile(tokenPath(version));
    }

    public void cleanup() throws IOException {
        cleanup(-1);
    }

    public void cleanup(int versionsToKeep) throws IOException {
        List<Long> versions = getAllVersions();
        if (versionsToKeep >= 0) {
            versions = versions.subList(0, Math.min(versions.size(), versionsToKeep));
        }
        HashSet<Long> keepers = new HashSet<Long>(versions);

        for (Path p : listDir(_root)) {
            Long v = parseVersion(p);
            if (v != null && !keepers.contains(v)) {
                deleteVersion(v);
            }
        }
    }

    /**
     * Sorted from most recent to oldest.
     */
    public List<Long> getAllVersions() throws IOException {
        List<Long> ret = new ArrayList<Long>();
        for (Path p : listDir(_root)) {
            String s = p.toString();
            if (s.endsWith(FINISHED_VERSION_SUFFIX) 
                && Paths.get(s.substring(0, s.length() - FINISHED_VERSION_SUFFIX.length())).toFile().exists()) {
                ret.add(validateAndGetVersion(p));
            }
        }
        Collections.sort(ret);
        Collections.reverse(ret);
        return ret;
    }

    private Path tokenPath(long version) {
        return _root.resolve("" + version + FINISHED_VERSION_SUFFIX).toAbsolutePath();
    }

    private long validateAndGetVersion(Path path) {
        Long v = parseVersion(path);
        if (v == null) {
            throw new RuntimeException(path + " is not a valid version");
        }
        return v;
    }

    private Long parseVersion(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(FINISHED_VERSION_SUFFIX)) {
            name = name.substring(0, name.length() - FINISHED_VERSION_SUFFIX.length());
        }
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private List<Path> listDir(Path dir) throws IOException {
        try (Stream<Path> filesList = Files.list(dir)) {
            return filesList
                .map(content -> content.toAbsolutePath())
                .collect(Collectors.toList());
        } catch (IOException e) {
            //Can happen if worker directory is deleted, most likely harmless
            LOG.debug("IOException when listing versioned store dir, ignoring", e);
            return Collections.emptyList();
        }
    }
}
