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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedFSOps implements IAdvancedFSOps {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedFSOps.class);
    
    /**
     * Factory to create a new AdvancedFSOps
     * @param conf the configuration of the process
     * @return the appropriate instance of the class for this config and environment.
     */
    public static AdvancedFSOps make(Map<String, Object> conf) {
        if (Utils.isOnWindows()) {
            return new AdvancedWindowsFSOps(conf);
        }
        if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            return new AdvancedRunAsUserFSOps(conf);
        }
        return new AdvancedFSOps(conf);
    }
    
    private static class AdvancedRunAsUserFSOps extends AdvancedFSOps {
        private final Map<String, Object> _conf;
        
        public AdvancedRunAsUserFSOps(Map<String, Object> conf) {
            super(conf);
            if (Utils.isOnWindows()) {
                throw new UnsupportedOperationException("ERROR: Windows doesn't support running workers as different users yet");
            }
            _conf = conf;
        }
        
        @Override
        public void setupBlobPermissions(File path, String user) throws IOException {
            String logPrefix = "setup blob permissions for " + path;
            ClientSupervisorUtils.processLauncherAndWait(_conf, user, Arrays.asList("blob", path.toString()), null, logPrefix);
        }
        
        @Override
        public void deleteIfExists(File path, String user, String logPrefix) throws IOException {
            String absolutePath = path.getAbsolutePath();
            if (Utils.checkFileExists(absolutePath)) {
                LOG.info("Deleting path (runAsUser) {}", absolutePath);
                if (user == null) {
                    user = Files.getOwner(path.toPath()).getName();
                }
                List<String> commands = new ArrayList<>();
                commands.add("rmr");
                commands.add(absolutePath);
                ClientSupervisorUtils.processLauncherAndWait(_conf, user, commands, null, logPrefix);

                if (Utils.checkFileExists(absolutePath)) {
                    // It's possible that permissions were not set properly on the directory, and
                    // the user who is *supposed* to own the dir does not. In this case, try the
                    // delete as the supervisor user.
                    Utils.forceDelete(absolutePath);
                    if (Utils.checkFileExists(absolutePath)) {
                        throw new RuntimeException(path + " was not deleted.");
                    }
                }
            }
        }

        @Override
        public void deleteIfExists(File path) throws IOException {
            deleteIfExists(path, null, "UNNAMED");
        }
        
        @Override
        public void setupStormCodeDir(String user, File path) throws IOException {
            ClientSupervisorUtils.setupStormCodeDir(_conf, user, path.getCanonicalPath());
        }

        @Override
        public void setupWorkerArtifactsDir(String user, File path) throws IOException {
            ClientSupervisorUtils.setupWorkerArtifactsDir(_conf, user, path.getCanonicalPath());
        }
    }
    
    /**
     * Operations that need to override the default ones when running on Windows
     *
     */
    private static class AdvancedWindowsFSOps extends AdvancedFSOps {

        public AdvancedWindowsFSOps(Map<String, Object> conf) {
            super(conf);
            if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                throw new RuntimeException("ERROR: Windows doesn't support running workers as different users yet");
            }
        }
        
        @Override
        public void restrictDirectoryPermissions(File dir) throws IOException {
            //NOOP, if windows gets support for run as user we will need to find a way to support this
        }
        
        @Override
        public void moveDirectoryPreferAtomic(File fromDir, File toDir) throws IOException {
            // Files/move with non-empty directory doesn't work well on Windows
            // This is not atomic but it does work
            FileUtils.moveDirectory(fromDir, toDir);
        }
        
        @Override
        public boolean supportsAtomicDirectoryMove() {
            // Files/move with non-empty directory doesn't work well on Windows
            // FileUtils.moveDirectory is not atomic
            return false;
        }
    }

    protected final boolean _symlinksDisabled;
    
    protected AdvancedFSOps(Map<String, Object> conf) {
        _symlinksDisabled = (boolean)conf.getOrDefault(Config.DISABLE_SYMLINKS, false);
    }

    /**
     * Set directory permissions to (OWNER)RWX (GROUP)R-X (OTHER)---
     * On some systems that do not support this, it may become a noop
     * @param dir the directory to change permissions on
     * @throws IOException on any error
     */
    public void restrictDirectoryPermissions(File dir) throws IOException {
        Set<PosixFilePermission> perms = new HashSet<>(
                Arrays.asList(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
                        PosixFilePermission.GROUP_EXECUTE));
        Files.setPosixFilePermissions(dir.toPath(), perms);
    }

    /**
     * Move fromDir to toDir, and try to make it an atomic move if possible
     * @param fromDir what to move
     * @param toDir where to move it from
     * @throws IOException on any error
     */
    public void moveDirectoryPreferAtomic(File fromDir, File toDir) throws IOException {
        FileUtils.forceMkdir(toDir);
        Files.move(fromDir.toPath(), toDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * @return true if an atomic directory move works, else false.
     */
    public boolean supportsAtomicDirectoryMove() {
        return true;
    }

    /**
     * Copy a directory
     * @param fromDir from where
     * @param toDir to where
     * @throws IOException on any error
     */
    public void copyDirectory(File fromDir, File toDir) throws IOException {
        FileUtils.copyDirectory(fromDir, toDir);
    }

    /**
     * Setup permissions properly for an internal blob store path
     * @param path the path to set the permissions on
     * @param user the user to change the permissions for
     * @throws IOException on any error
     */
    public void setupBlobPermissions(File path, String user) throws IOException {
        //Normally this is a NOOP
    }

    /**
     * Delete a file or a directory and all of the children. If it exists.
     * @param path what to delete
     * @param user who to delete it as if doing it as someone else is supported
     * @param logPrefix if an external process needs to be launched to delete
     * the object what prefix to include in the logs
     * @throws IOException on any error.
     */
    public void deleteIfExists(File path, String user, String logPrefix) throws IOException {
        //by default no need to do this as a different user
        deleteIfExists(path);
    }

    /**
     * Delete a file or a directory and all of the children. If it exists.
     * @param path what to delete
     * @throws IOException on any error.
     */
    public void deleteIfExists(File path) throws IOException {
        LOG.info("Deleting path {}", path);
        Path p = path.toPath();
        if (Files.exists(p)) {
            try {
                FileUtils.forceDelete(path);
            } catch (FileNotFoundException ignored) {}
        }
    }

    /**
     * Setup the permissions for the storm code dir
     * @param user the user that owns the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    public void setupStormCodeDir(String user, File path) throws IOException {
        //By default this is a NOOP
    }

    /**
     * Setup the permissions for the worker artifacts dirs
     * @param user the user that owns the topology
     * @param path the directory to set the permissions on
     * @throws IOException on any error
     */
    public void setupWorkerArtifactsDir(String user, File path) throws IOException {
        //By default this is a NOOP
    }

    /**
     * Sanity check if everything the topology needs is there for it to run.
     * @param conf the config of the supervisor
     * @param topologyId the ID of the topology
     * @return true if everything is there, else false
     * @throws IOException on any error
     */
    public boolean doRequiredTopoFilesExist(Map<String, Object> conf, String topologyId) throws IOException {
        return ClientSupervisorUtils.doRequiredTopoFilesExist(conf, topologyId);
    }

    /**
     * Makes a directory, including any necessary but nonexistent parent
     * directories.
     *
     * @param path the directory to create
     * @throws IOException on any error
     */
    public void forceMkdir(File path) throws IOException {
        FileUtils.forceMkdir(path);
    }

    /**
     * Check if a file exists or not
     * @param path the path to check
     * @return true if it exists else false
     * @throws IOException on any error.
     */
    public boolean fileExists(File path) throws IOException {
        return path.exists();
    }

    /**
     * Get a writer for the given location
     * @param file the file to write to
     * @return the Writer to use.
     * @throws IOException on any error
     */
    public Writer getWriter(File file) throws IOException {
        return new FileWriter(file);
    }

    /**
     * Get an output stream to write to a given file
     * @param file the file to write to
     * @return an OutputStream for that file
     * @throws IOException on any error
     */
    public OutputStream getOutputStream(File file) throws IOException {
        return new FileOutputStream(file);
    }

    /**
     * Dump a string to a file
     * @param location where to write to
     * @param data the data to write
     * @throws IOException on any error
     */
    public void dump(File location, String data) throws IOException {
        File parent = location.getParentFile();
        if (!parent.exists()) {
            forceMkdir(parent);
        }
        try (Writer w = getWriter(location)) {
            w.write(data);
        }
    }

    /**
     * Read the contents of a file into a String
     * @param location the file to read
     * @return the contents of the file
     * @throws IOException on any error
     */
    @Override
    public String slurpString(File location) throws IOException {
        return FileUtils.readFileToString(location, "UTF-8");
    }

    /**
     * Read the contents of a file into a byte array.
     * @param location the file to read
     * @return the contents of the file
     * @throws IOException on any error
     */
    @Override
    public byte[] slurp(File location) throws IOException {
        return FileUtils.readFileToByteArray(location);
    }

    /**
     * Create a symbolic link pointing at target
     * @param link the link to create
     * @param target where it should point to
     * @throws IOException on any error.
     */
    @Override
    public void createSymlink(File link, File target) throws IOException {
        if (_symlinksDisabled) {
            throw new IOException("Symlinks have been disabled, this should not be called");
        }
        Path plink = link.toPath().toAbsolutePath();
        Path ptarget = target.toPath().toAbsolutePath();
        LOG.debug("Creating symlink [{}] to [{}]", plink, ptarget);
        if (Files.exists(plink)) {
            if (Files.isSameFile(plink, ptarget)) {
                //It already points where we want it to
                return;
            }
            FileUtils.forceDelete(link);
        }
        Files.createSymbolicLink(plink, ptarget);
    }
    
}
