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

package org.apache.storm.localizer;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.supervisor.IAdvancedFSOps;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.WrappedAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a resource that is localized on the supervisor. A localized resource has a .current symlink to the current version file which
 * is named filename.{current version}. There is also a filename.version which contains the latest version.
 */
public class LocalizedResource extends LocallyCachedBlob {
    @VisibleForTesting
    static final String CURRENT_BLOB_SUFFIX = ".current";
    @VisibleForTesting
    static final String BLOB_VERSION_SUFFIX = ".version";
    @VisibleForTesting
    static final String FILECACHE = "filecache";
    @VisibleForTesting
    static final String USERCACHE = "usercache";
    // sub directories to store either files or uncompressed archives respectively
    @VisibleForTesting
    static final String FILESDIR = "files";
    @VisibleForTesting
    static final String ARCHIVESDIR = "archives";
    private static final Logger LOG = LoggerFactory.getLogger(LocalizedResource.class);
    private static final String TO_UNCOMPRESS = "_tmp_";
    private static final Pattern VERSION_FILE_PATTERN = Pattern.compile("^(.+)\\.(\\d+)$");
    // filesystem path to the resource
    private final Path baseDir;
    private final Path versionFilePath;
    private final Path symlinkPath;
    private final boolean shouldUncompress;
    private final IAdvancedFSOps fsOps;
    private final String user;
    private final Map<String, Object> conf;
    private final boolean symLinksDisabled;
    // size of the resource
    private long size = -1;

    LocalizedResource(String key, Path localBaseDir, boolean shouldUncompress, IAdvancedFSOps fsOps, Map<String, Object> conf,
                      String user, StormMetricsRegistry metricRegistry) {
        super(key + (shouldUncompress ? " archive" : " file"), key, metricRegistry);
        Path base = getLocalUserFileCacheDir(localBaseDir, user);
        this.baseDir = shouldUncompress ? getCacheDirForArchives(base) : getCacheDirForFiles(base);
        this.conf = conf;
        this.symLinksDisabled = (boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false);
        this.user = user;
        this.fsOps = fsOps;
        versionFilePath = constructVersionFileName(baseDir, key);
        symlinkPath = constructBlobCurrentSymlinkName(baseDir, key);
        this.shouldUncompress = shouldUncompress;
        //Set the size in case we are recovering an already downloaded object
        setSize();
    }

    private static Path constructVersionFileName(Path baseDir, String key) {
        return baseDir.resolve(key + BLOB_VERSION_SUFFIX);
    }

    @VisibleForTesting
    static long localVersionOfBlob(Path versionFile) {
        long currentVersion = -1;
        if (Files.exists(versionFile) && !(Files.isDirectory(versionFile))) {
            try (BufferedReader br = new BufferedReader(new FileReader(versionFile.toFile()))) {
                String line = br.readLine();
                currentVersion = Long.parseLong(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return currentVersion;
    }

    private static Path constructBlobCurrentSymlinkName(Path baseDir, String key) {
        return baseDir.resolve(key + CURRENT_BLOB_SUFFIX);
    }

    private static Path constructBlobWithVersionFileName(Path baseDir, String key, long version) {
        return baseDir.resolve(key + "." + version);
    }

    static Collection<String> getLocalizedUsers(Path localBaseDir) throws IOException {
        Path userCacheDir = getUserCacheDir(localBaseDir);
        if (!Files.exists(userCacheDir)) {
            return Collections.emptyList();
        }
        return Files.list(userCacheDir).map((p) -> p.getFileName().toString()).collect(Collectors.toList());
    }

    static void completelyRemoveUnusedUser(Path localBaseDir, String user) throws IOException {
        Path localUserDir = getLocalUserDir(localBaseDir, user);
        LOG.info("completelyRemoveUnusedUser {} for directory {}", user, localUserDir);

        Path userFileCacheDir = getLocalUserFileCacheDir(localBaseDir, user);
        // baseDir/supervisor/usercache/user1/filecache/files
        Files.deleteIfExists(getCacheDirForFiles(userFileCacheDir));
        // baseDir/supervisor/usercache/user1/filecache/archives
        Files.deleteIfExists(getCacheDirForArchives(userFileCacheDir));
        // baseDir/supervisor/usercache/user1/filecache
        Files.deleteIfExists(userFileCacheDir);
        // baseDir/supervisor/usercache/user1
        Files.deleteIfExists(localUserDir);
    }

    static List<String> getLocalizedArchiveKeys(Path localBaseDir, String user) throws IOException {
        Path dir = getCacheDirForArchives(getLocalUserFileCacheDir(localBaseDir, user));
        return readKeysFromDir(dir);
    }

    static List<String> getLocalizedFileKeys(Path localBaseDir, String user) throws IOException {
        Path dir = getCacheDirForFiles(getLocalUserFileCacheDir(localBaseDir, user));
        return readKeysFromDir(dir);
    }

    // Looks for files in the directory with .current suffix
    private static List<String> readKeysFromDir(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return Collections.emptyList();
        }
        return Files.list(dir)
                    .map((p) -> p.getFileName().toString())
                    .filter((name) -> name.toLowerCase().endsWith(CURRENT_BLOB_SUFFIX))
                    .map((key) -> {
                        int p = key.lastIndexOf('.');
                        if (p > 0) {
                            key = key.substring(0, p);
                        }
                        return key;
                    })
                    .collect(Collectors.toList());
    }

    // baseDir/supervisor/usercache/
    private static Path getUserCacheDir(Path localBaseDir) {
        return localBaseDir.resolve(USERCACHE);
    }

    // baseDir/supervisor/usercache/user1/
    static Path getLocalUserDir(Path localBaseDir, String userName) {
        return getUserCacheDir(localBaseDir).resolve(userName);
    }

    // baseDir/supervisor/usercache/user1/filecache
    static Path getLocalUserFileCacheDir(Path localBaseDir, String userName) {
        return getLocalUserDir(localBaseDir, userName).resolve(FILECACHE);
    }

    // baseDir/supervisor/usercache/user1/filecache/files
    private static Path getCacheDirForFiles(Path dir) {
        return dir.resolve(FILESDIR);
    }

    // get the directory to put uncompressed archives in
    // baseDir/supervisor/usercache/user1/filecache/archives
    private static Path getCacheDirForArchives(Path dir) {
        return dir.resolve(ARCHIVESDIR);
    }

    Path getCurrentSymlinkPath() {
        return symlinkPath;
    }

    @VisibleForTesting
    Path getFilePathWithVersion() {
        return constructBlobWithVersionFileName(baseDir, getKey(), getLocalVersion());
    }

    private void setSize() {
        // we trust that the file exists
        Path withVersion = getFilePathWithVersion();
        size = ServerUtils.getDiskUsage(withVersion.toFile());
        LOG.debug("size of {} is: {}", withVersion, size);
    }

    @VisibleForTesting
    protected void setSize(long size) {
        this.size = size;
    }

    @Override
    public long getLocalVersion() {
        return localVersionOfBlob(versionFilePath);
    }

    @Override
    public long getRemoteVersion(ClientBlobStore store) throws KeyNotFoundException, AuthorizationException {
        return ServerUtils.nimbusVersionOfBlob(getKey(), store);
    }

    @Override
    public long fetchUnzipToTemp(ClientBlobStore store) throws IOException, KeyNotFoundException, AuthorizationException {
        String key = getKey();
        ReadableBlobMeta meta = store.getBlobMeta(key);
        if (!ServerUtils.canUserReadBlob(meta, user, conf)) {
            throw new WrappedAuthorizationException(user + " does not have READ access to " + key);
        }

        DownloadMeta downloadMeta = fetch(store, key, v -> {
                Path path = shouldUncompress ? tmpOutputLocation() : constructBlobWithVersionFileName(baseDir, getKey(), v);
                // we need to download to temp file and then unpack into the one requested
                Path parent = path.getParent();
                if (!Files.exists(parent)) {
                    //There is a race here that we can still lose
                    try {
                        Files.createDirectories(parent);
                    } catch (FileAlreadyExistsException e) {
                        //Ignored
                    } catch (IOException e) {
                        LOG.error("Failed to create parent directory {}", parent, e);
                        throw e;
                    }
                }
                return path;
            },
            FileOutputStream::new
        );

        Path finalLocation = downloadMeta.getDownloadPath();
        if (shouldUncompress) {
            Path downloadFile = finalLocation;
            finalLocation = constructBlobWithVersionFileName(baseDir, getKey(), downloadMeta.getVersion());
            ServerUtils.unpack(downloadFile.toFile(), finalLocation.toFile(), symLinksDisabled);
            LOG.debug("Uncompressed {} to: {}", downloadFile, finalLocation);
        }
        setBlobPermissions(conf, user, finalLocation);
        return downloadMeta.getVersion();
    }

    @Override
    protected void commitNewVersion(long version) throws IOException {
        String key = getKey();
        LOG.info("Blob: {} updated to version {} from version {}", key, version, getLocalVersion());
        Path localVersionFile = versionFilePath;
        // The false parameter ensures overwriting the version file, not appending
        try (PrintWriter writer = new PrintWriter(
            new BufferedWriter(new FileWriter(localVersionFile.toFile(), false)))) {
            writer.println(version);
        }
        setBlobPermissions(conf, user, localVersionFile);

        // Update the key.current symlink. First create tmp symlink and do
        // move of tmp to current so that the operation is atomic.
        Path tmpSymlink = tmpSymlinkLocation();
        Path targetOfSymlink = constructBlobWithVersionFileName(baseDir, getKey(), version);
        LOG.debug("Creating a symlink @{} linking to: {}", tmpSymlink, targetOfSymlink);
        Files.createSymbolicLink(tmpSymlink, targetOfSymlink);

        Path currentSymLink = getCurrentSymlinkPath();
        Files.move(tmpSymlink, currentSymLink, ATOMIC_MOVE);
        //Update the size of the objects
        setSize();
    }

    private void setBlobPermissions(Map<String, Object> conf, String user, Path path)
        throws IOException {

        if (!ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            return;
        }
        String wlCommand = ObjectReader.getString(conf.get(Config.SUPERVISOR_WORKER_LAUNCHER), "");
        if (wlCommand.isEmpty()) {
            String stormHome = System.getProperty(ConfigUtils.STORM_HOME);
            wlCommand = stormHome + "/bin/worker-launcher";
        }
        List<String> command = new ArrayList<>(Arrays.asList(wlCommand, user, "blob", path.toString()));

        String[] commandArray = command.toArray(new String[command.size()]);
        ShellUtils.ShellCommandExecutor shExec = new ShellUtils.ShellCommandExecutor(commandArray);
        LOG.debug("Setting blob permissions, command: {}", Arrays.toString(commandArray));

        try {
            shExec.execute();
            LOG.debug("output: {}", shExec.getOutput());
        } catch (ShellUtils.ExitCodeException e) {
            int exitCode = shExec.getExitCode();
            LOG.warn("Exit code from worker-launcher is: {}", exitCode, e);
            LOG.debug("output: {}", shExec.getOutput());
            throw new IOException("Setting blob permissions failed"
                                  + " (exitCode=" + exitCode + ") with output: " + shExec.getOutput(), e);
        }
    }

    private Path tmpOutputLocation() {
        return baseDir.resolve(Paths.get(LocalizedResource.TO_UNCOMPRESS + getKey()));
    }

    private Path tmpSymlinkLocation() {
        return baseDir.resolve(Paths.get(LocalizedResource.TO_UNCOMPRESS + getKey() + CURRENT_BLOB_SUFFIX));
    }

    @Override
    public void cleanupOrphanedData() throws IOException {
        //There are a few possible files that we would want to clean up
        //baseDir + "/" + "_tmp_" + baseName
        //baseDir + "/" + "_tmp_" + baseName + ".current"
        //baseDir + "/" + baseName.<VERSION>
        //baseDir + "/" + baseName.current
        //baseDir + "/" + baseName.version
        //In general we always want to delete the _tmp_ files if they are there.

        Path tmpOutput = tmpOutputLocation();
        Files.deleteIfExists(tmpOutput);
        Path tmpSym = tmpSymlinkLocation();
        Files.deleteIfExists(tmpSym);

        try {
            String baseName = getKey();
            long version = getLocalVersion();
            Path current = getCurrentSymlinkPath();

            //If .current and .version do not match, we roll back the .version file to match
            // what .current is pointing to.
            if (Files.exists(current) && Files.isSymbolicLink(current)) {
                Path versionFile = Files.readSymbolicLink(current);
                Matcher m = VERSION_FILE_PATTERN.matcher(versionFile.getFileName().toString());
                if (m.matches()) {
                    long foundVersion = Long.valueOf(m.group(2));
                    if (foundVersion != version) {
                        LOG.error("{} does not match the version file so fix the version file", current);
                        //The versions are different so roll back to whatever current is
                        try (PrintWriter restoreWriter = new PrintWriter(
                            new BufferedWriter(new FileWriter(versionFilePath.toFile(), false)))) {
                            restoreWriter.println(foundVersion);
                        }
                        version = foundVersion;
                    }
                }
            }

            // Finally delete any baseName.<VERSION> files that are not pointed to by the current version
            final long finalVersion = version;
            LOG.debug("Looking to clean up after {} in {}", getKey(), baseDir);
            try (DirectoryStream<Path> ds = fsOps.newDirectoryStream(baseDir, (path) -> {
                Matcher m = VERSION_FILE_PATTERN.matcher(path.getFileName().toString());
                if (m.matches()) {
                    long foundVersion = Long.valueOf(m.group(2));
                    return m.group(1).equals(baseName) && foundVersion != finalVersion;
                }
                return false;
            })) {
                for (Path p : ds) {
                    LOG.info("Cleaning up old localized resource file {}", p);
                    if (Files.isDirectory(p)) {
                        FileUtils.deleteDirectory(p.toFile());
                    } else {
                        fsOps.deleteIfExists(p.toFile());
                    }
                }
            }
        } catch (NoSuchFileException e) {
            LOG.warn("Nothing to cleanup with baseDir {} even though we expected there to be something there", baseDir);
        }
    }

    @Override
    public void completelyRemove() throws IOException {
        Path fileWithVersion = getFilePathWithVersion();
        Path currentSymLink = getCurrentSymlinkPath();

        if (shouldUncompress) {
            if (Files.exists(fileWithVersion)) {
                // this doesn't follow symlinks, which is what we want
                FileUtils.deleteDirectory(fileWithVersion.toFile());
            }
        } else {
            Files.deleteIfExists(fileWithVersion);
        }
        Files.deleteIfExists(currentSymLink);
        Files.deleteIfExists(versionFilePath);
    }

    @Override
    public long getSizeOnDisk() {
        return size;
    }

    @Override
    public boolean isFullyDownloaded() {
        return Files.exists(getFilePathWithVersion())
               && Files.exists(getCurrentSymlinkPath())
               && Files.exists(versionFilePath);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LocalizedResource) {
            LocalizedResource l = (LocalizedResource) other;
            return getKey().equals(l.getKey()) && shouldUncompress == l.shouldUncompress && baseDir.equals(l.baseDir);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode() + Boolean.hashCode(shouldUncompress) + baseDir.hashCode();
    }

    @Override
    public String toString() {
        return this.user + ":" + getKey();
    }
}
