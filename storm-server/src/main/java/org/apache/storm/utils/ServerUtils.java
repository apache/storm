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

package org.apache.storm.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.security.auth.Subject;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.blobstore.LocalModeClientBlobStore;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerUtils {
    public static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);

    public static final boolean IS_ON_WINDOWS = "Windows_NT".equals(System.getenv("OS"));

    public static final int SIGKILL = 9;
    public static final int SIGTERM = 15;

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ServerUtils _instance = new ServerUtils();

    /**
     * Provide an instance of this class for delegates to use.
     * To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a ServerUtils instance
     * @return the previously set instance
     */
    public static ServerUtils setInstance(ServerUtils u) {
        ServerUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    public static <T> List<T> interleaveAll(List<List<T>> nodeList) {
        if (nodeList != null && nodeList.size() > 0) {
            List<T> first = new ArrayList<T>();
            List<List<T>> rest = new ArrayList<List<T>>();
            for (List<T> node : nodeList) {
                if (node != null && node.size() > 0) {
                    first.add(node.get(0));
                    rest.add(node.subList(1, node.size()));
                }
            }
            List<T> interleaveRest = interleaveAll(rest);
            if (interleaveRest != null) {
                first.addAll(interleaveRest);
            }
            return first;
        }
        return null;
    }

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf, NimbusInfo nimbusInfo, ILeaderElector leaderElector) {
        return getNimbusBlobStore(conf, null, nimbusInfo, leaderElector);
    }

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf, String baseDir, NimbusInfo nimbusInfo, ILeaderElector leaderElector) {
        String type = (String)conf.get(DaemonConfig.NIMBUS_BLOBSTORE);
        if (type == null) {
            type = LocalFsBlobStore.class.getName();
        }
        BlobStore store = (BlobStore) ReflectionUtils.newInstance(type);
        Map<String, Object> nconf = new HashMap<>(conf);
        // only enable cleanup of blobstore on nimbus
        nconf.put(Config.BLOBSTORE_CLEANUP_ENABLE, Boolean.TRUE);

        if (store != null) {
            // store can be null during testing when mocking utils.
            store.prepare(nconf, baseDir, nimbusInfo, leaderElector);
        }
        return store;
    }

    public static boolean isAbsolutePath(String path) {
        return Paths.get(path).isAbsolute();
    }

    /**
     * Returns the combined string, escaped for posix shell.
     *
     * @param command the list of strings to be combined
     * @return the resulting command string
     */
    public static String shellCmd(List<String> command) {
        List<String> changedCommands = new ArrayList<>(command.size());
        for (String str : command) {
            if (str == null) {
                continue;
            }
            changedCommands.add("'" + str.replaceAll("'", "'\"'\"'") + "'");
        }
        return StringUtils.join(changedCommands, " ");
    }

    /**
     * Takes an input dir or file and returns the disk usage on that local directory. Very basic implementation.
     *
     * @param dir The input dir to get the disk space of this local dir
     * @return The total disk space of the input local directory
     */
    public static long getDU(File dir) {
        long size = 0;
        if (!dir.exists()) {
            return 0;
        }
        if (!dir.isDirectory()) {
            return dir.length();
        } else {
            File[] allFiles = dir.listFiles();
            if (allFiles != null) {
                for (int i = 0; i < allFiles.length; i++) {
                    boolean isSymLink;
                    try {
                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
                    } catch (IOException ioe) {
                        isSymLink = true;
                    }
                    if (!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }
            return size;
        }
    }

    public static ClientBlobStore getClientBlobStoreForSupervisor(Map<String, Object> conf) {
        ClientBlobStore store;
        if (ConfigUtils.isLocalMode(conf)) {
            store = new LocalModeClientBlobStore(getNimbusBlobStore(conf, null, null));
        } else {
            store = (ClientBlobStore) ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.SUPERVISOR_BLOBSTORE));
        }
        store.prepare(conf);
        return store;
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
     *
     * @param key
     * @param localFile
     * @param cb
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     * @throws IOException
     */
    public static void downloadResourcesAsSupervisor(String key, String localFile,
                                                     ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException, IOException {
        _instance.downloadResourcesAsSupervisorImpl(key, localFile, cb);
    }

    /**
     * Extract dir from the jar to destdir
     *
     * @param jarpath Path to the jar file
     * @param dir     Directory in the jar to pull out
     * @param destdir Path to the directory where the extracted directory will be put
     */
    public static void extractDirFromJar(String jarpath, String dir, File destdir) {
        _instance.extractDirFromJarImpl(jarpath, dir, destdir);
    }

    /**
     * Returns the value of java.class.path System property. Kept separate for testing.
     *
     * @return the classpath
     */
    public static String currentClasspath() {
        return _instance.currentClasspathImpl();
    }

    /**
     * Determines if a zip archive contains a particular directory.
     *
     * @param zipfile path to the zipped file
     * @param target  directory being looked for in the zip.
     * @return boolean whether or not the directory exists in the zip.
     */
    public static boolean zipDoesContainDir(String zipfile, String target) throws IOException {
        List<ZipEntry> entries = (List<ZipEntry>) Collections.list(new ZipFile(zipfile).entries());

        String targetDir = target + "/";
        for (ZipEntry entry : entries) {
            String name = entry.getName();
            if (name.startsWith(targetDir)) {
                return true;
            }
        }

        return false;
    }

    public static String getFileOwner(String path) throws IOException {
        return Files.getOwner(FileSystems.getDefault().getPath(path)).getName();
    }

    public static String containerFilePath(String dir) {
        return dir + File.separator + "launch_container.sh";
    }

    public static String scriptFilePath(String dir) {
        return dir + File.separator + "storm-worker-script.sh";
    }

    /**
     * Writes a posix shell script file to be executed in its own process.
     *
     * @param dir         the directory under which the script is to be written
     * @param command     the command the script is to execute
     * @param environment optional environment variables to set before running the script's command. May be  null.
     * @return the path to the script that has been written
     */
    public static String writeScript(String dir, List<String> command,
                                     Map<String, String> environment) throws IOException {
        String path = scriptFilePath(dir);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            out.write("#!/bin/bash");
            out.newLine();
            if (environment != null) {
                for (String k : environment.keySet()) {
                    String v = environment.get(k);
                    if (v == null) {
                        v = "";
                    }
                    out.write(shellCmd(
                        Arrays.asList(
                            "export", k + "=" + v)));
                    out.write(";");
                    out.newLine();
                }
            }
            out.newLine();
            out.write("exec " + shellCmd(command) + ";");
        }
        return path;
    }

    public static int execCommand(String... command) throws ExecuteException, IOException {
        CommandLine cmd = new CommandLine(command[0]);
        for (int i = 1; i < command.length; i++) {
            cmd.addArgument(command[i]);
        }

        DefaultExecutor exec = new DefaultExecutor();
        return exec.execute(cmd);
    }

    public static void sendSignalToProcess(long lpid, int signum) throws IOException {
        String pid = Long.toString(lpid);
        try {
            if (Utils.isOnWindows()) {
                if (signum == SIGKILL) {
                    execCommand("taskkill", "/f", "/pid", pid);
                } else {
                    execCommand("taskkill", "/pid", pid);
                }
            } else {
                execCommand("kill", "-" + signum, pid);
            }
        } catch (ExecuteException e) {
            LOG.info("Error when trying to kill {}. Process is probably already dead.", pid);
        } catch (IOException e) {
            LOG.info("IOException Error when trying to kill {}.", pid);
            throw e;
        }
    }

    public static void killProcessWithSigTerm(String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGTERM);
    }

    public static void forceKillProcess(String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGKILL);
    }

    public static long nimbusVersionOfBlob(String key, ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException {
        long nimbusBlobVersion = 0;
        ReadableBlobMeta metadata = cb.getBlobMeta(key);
        nimbusBlobVersion = metadata.get_version();
        return nimbusBlobVersion;
    }

    public static boolean canUserReadBlob(ReadableBlobMeta meta, String user, Map<String, Object> conf) {

        if (!ObjectReader.getBoolean(conf.get(Config.STORM_BLOBSTORE_ACL_VALIDATION_ENABLED), false)) {
            return true;
        }

        SettableBlobMeta settable = meta.get_settable();
        for (AccessControl acl : settable.get_acl()) {
            if (acl.get_type().equals(AccessControlType.OTHER) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
            if (acl.get_name().equals(user) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Unpack matching files from a jar. Entries inside the jar that do not match the given pattern will be skipped.
     *
     * @param jarFile the .jar file to unpack
     * @param toDir   the destination directory into which to unpack the jar
     */
    public static void unJar(File jarFile, File toDir) throws IOException {
        try (JarFile jar = new JarFile(jarFile)) {
            extractZipFile(jar, toDir, null);
        }
    }

    /**
     * Ensure the existence of a given directory.
     *
     * @throws IOException if it cannot be created and does not already exist
     */
    private static void ensureDirectory(File dir) throws IOException {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " +
                                  dir.toString());
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input.
     * @param untarDir The untar directory where to untar the tar file.
     * @param symlinksDisabled true if symlinks should be disabled, else false.
     * @throws IOException
     */
    public static void unTar(File inFile, File untarDir, boolean symlinksDisabled) throws IOException {
        ensureDirectory(untarDir);

        boolean gzipped = inFile.toString().endsWith("gz");
        if (Utils.isOnWindows() || symlinksDisabled) {
            // Tar is not native to Windows. Use simple Java based implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, untarDir, gzipped, symlinksDisabled);
        } else {
            // spawn tar utility to untar archive for full fledged unix behavior such
            // as resolving symlinks in tar archives
            unTarUsingTar(inFile, untarDir, gzipped);
        }
    }

    private static void unTarUsingTar(File inFile, File untarDir,
                                      boolean gzipped) throws IOException {
        StringBuffer untarCommand = new StringBuffer();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(inFile.toString());
            untarCommand.append("' | (");
        }
        untarCommand.append("cd '");
        untarCommand.append(untarDir.toString());
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");

        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(inFile.toString());
        }
        String[] shellCmd = { "bash", "-c", untarCommand.toString() };
        ShellUtils.ShellCommandExecutor shexec = new ShellUtils.ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile +
                                  ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir,
                                       boolean gzipped, boolean symlinksDisabled) throws IOException {
        final String base = untarDir.getCanonicalPath();
        LOG.trace("java untar {} to {}", inFile, base);
        InputStream inputStream = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(
                    new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }
            try (TarArchiveInputStream tis = new TarArchiveInputStream(inputStream)) {
                for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; ) {
                    unpackEntries(tis, entry, untarDir, base, symlinksDisabled);
                    entry = tis.getNextTarEntry();
                }
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir, final String base,
                                      boolean symlinksDisabled) throws IOException {
        File target = new File(outputDir, entry.getName());
        String found = target.getCanonicalPath();
        if (!found.startsWith(base)) {
            LOG.error("Invalid location {} is outside of {}", found, base);
            return;
        }
        if (entry.isDirectory()) {
            LOG.trace("Extracting dir {}", target);
            ensureDirectory(target);
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, target, base, symlinksDisabled);
            }
        } else if (entry.isSymbolicLink()) {
            if (symlinksDisabled) {
                LOG.info("Symlinks disabled skipping {}", target);
            } else {
                Path src = target.toPath();
                Path dest = Paths.get(entry.getLinkName());
                LOG.trace("Extracting sym link {} to {}", target, dest);
                // Create symbolic link relative to tar parent dir
                Files.createSymbolicLink(src, dest);
            }
        } else if (entry.isFile()) {
            LOG.trace("Extracting file {}", target);
            ensureDirectory(target.getParentFile());
            try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(target))) {
                IOUtils.copy(tis, outputStream);
            }
        } else {
            LOG.error("{} is not a currently supported tar entry type.", entry);
        }

        Path p = target.toPath();
        if (Files.exists(p)) {
            try {
                //We created it so lets chmod it properly
                int mode = entry.getMode();
                Files.setPosixFilePermissions(p, parsePerms(mode));
            } catch (UnsupportedOperationException e) {
                //Ignored the file system we are on does not support this, so don't do it.
            }
        }
    }

    private static Set<PosixFilePermission> parsePerms(int mode) {
        Set<PosixFilePermission> ret = new HashSet<>();
        if ((mode & 0001) > 0) {
            ret.add(PosixFilePermission.OTHERS_EXECUTE);
        }
        if ((mode & 0002) > 0) {
            ret.add(PosixFilePermission.OTHERS_WRITE);
        }
        if ((mode & 0004) > 0) {
            ret.add(PosixFilePermission.OTHERS_READ);
        }
        if ((mode & 0010) > 0) {
            ret.add(PosixFilePermission.GROUP_EXECUTE);
        }
        if ((mode & 0020) > 0) {
            ret.add(PosixFilePermission.GROUP_WRITE);
        }
        if ((mode & 0040) > 0) {
            ret.add(PosixFilePermission.GROUP_READ);
        }
        if ((mode & 0100) > 0) {
            ret.add(PosixFilePermission.OWNER_EXECUTE);
        }
        if ((mode & 0200) > 0) {
            ret.add(PosixFilePermission.OWNER_WRITE);
        }
        if ((mode & 0400) > 0) {
            ret.add(PosixFilePermission.OWNER_READ);
        }
        return ret;
    }

    public static void unpack(File localrsrc, File dst, boolean symLinksDisabled) throws IOException {
        String lowerDst = localrsrc.getName().toLowerCase();
        if (lowerDst.endsWith(".jar") ||
            lowerDst.endsWith("_jar")) {
            unJar(localrsrc, dst);
        } else if (lowerDst.endsWith(".zip") ||
            lowerDst.endsWith("_zip")) {
            unZip(localrsrc, dst);
        } else if (lowerDst.endsWith(".tar.gz") ||
            lowerDst.endsWith("_tar_gz") ||
            lowerDst.endsWith(".tgz") ||
            lowerDst.endsWith("_tgz") ||
            lowerDst.endsWith(".tar") ||
            lowerDst.endsWith("_tar")) {
            unTar(localrsrc, dst, symLinksDisabled);
        } else {
            LOG.warn("Cannot unpack " + localrsrc);
            if (!localrsrc.renameTo(dst)) {
                throw new IOException("Unable to rename file: [" + localrsrc
                                      + "] to [" + dst + "]");
            }
        }
        if (localrsrc.isFile()) {
            localrsrc.delete();
        }
    }

    private static void extractZipFile(ZipFile zipFile, File toDir, String prefix) throws IOException {
        ensureDirectory(toDir);
        final String base = toDir.getCanonicalPath();

        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (!entry.isDirectory()) {
                if (prefix != null && !entry.getName().startsWith(prefix)) {
                    //No need to extract it, it is not what we are looking for.
                    continue;
                }
                File file = new File(toDir, entry.getName());
                String found = file.getCanonicalPath();
                if (!found.startsWith(base)) {
                    LOG.error("Invalid location {} is outside of {}", found, base);
                    continue;
                }

                try (InputStream in = zipFile.getInputStream(entry)) {
                    ensureDirectory(file.getParentFile());
                    try (OutputStream out = new FileOutputStream(file)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }

    /**
     * Given a File input it will unzip the file in a the unzip directory passed as the second parameter.
     *
     * @param inFile   The zip file as input
     * @param toDir The unzip directory where to unzip the zip file.
     * @throws IOException
     */
    public static void unZip(File inFile, File toDir) throws IOException {
        try (ZipFile zipFile = new ZipFile(inFile)) {
            extractZipFile(zipFile, toDir, null);
        }
    }

    /**
     * Given a zip File input it will return its size Only works for zip files whose uncompressed size is less than 4 GB, otherwise returns
     * the size module 2^32, per gzip specifications
     *
     * @param myFile The zip file as input
     * @return zip file size as a long
     *
     * @throws IOException
     */
    public static long zipFileSize(File myFile) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(myFile, "r")) {
            raf.seek(raf.length() - 4);
            long b4 = raf.read();
            long b3 = raf.read();
            long b2 = raf.read();
            long b1 = raf.read();
            return (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;
        }
    }

    private static boolean downloadResourcesAsSupervisorAttempt(ClientBlobStore cb, String key, String localFile) {
        boolean isSuccess = false;
        try (FileOutputStream out = new FileOutputStream(localFile);
             InputStreamWithMeta in = cb.getBlob(key);) {
            long fileSize = in.getFileLength();

            byte[] buffer = new byte[1024];
            int len;
            int downloadFileSize = 0;
            while ((len = in.read(buffer)) >= 0) {
                out.write(buffer, 0, len);
                downloadFileSize += len;
            }

            isSuccess = (fileSize == downloadFileSize);
        } catch (TException | IOException e) {
            LOG.error("An exception happened while downloading {} from blob store.", localFile, e);
        }
        if (!isSuccess) {
            try {
                Files.deleteIfExists(Paths.get(localFile));
            } catch (IOException ex) {
                LOG.error("Failed trying to delete the partially downloaded {}", localFile, ex);
            }
        }
        return isSuccess;
    }

    /**
     * Check if the scheduler is resource aware or not.
     *
     * @param conf The configuration
     * @return True if it's resource aware; false otherwise
     */
    public static boolean isRAS(Map<String, Object> conf) {
        if (conf.containsKey(DaemonConfig.STORM_SCHEDULER)) {
            if (conf.get(DaemonConfig.STORM_SCHEDULER).equals("org.apache.storm.scheduler.resource.ResourceAwareScheduler")) {
                return true;
            }
        }
        return false;
    }

    public static int getEstimatedWorkerCountForRASTopo(Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        Double defaultWorkerMaxHeap = ObjectReader.getDouble(topoConf.get(Config.WORKER_HEAP_MEMORY_MB), 768d);
        Double topologyWorkerMaxHeap = ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB), defaultWorkerMaxHeap);
        return (int) Math.ceil(getEstimatedTotalHeapMemoryRequiredByTopo(topoConf, topology) / topologyWorkerMaxHeap);
    }

    public static double getEstimatedTotalHeapMemoryRequiredByTopo(Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        Map<String, Integer> componentParallelism = getComponentParallelism(topoConf, topology);
        double totalMemoryRequired = 0.0;

        for (Map.Entry<String, NormalizedResourceRequest> entry : ResourceUtils.getBoltsResources(topology, topoConf).entrySet()) {
            int parallelism = componentParallelism.getOrDefault(entry.getKey(), 1);
            double memoryRequirement = entry.getValue().getOnHeapMemoryMb();
            totalMemoryRequired += memoryRequirement * parallelism;
        }

        for (Map.Entry<String, NormalizedResourceRequest> entry : ResourceUtils.getSpoutsResources(topology, topoConf).entrySet()) {
            int parallelism = componentParallelism.getOrDefault(entry.getKey(), 1);
            double memoryRequirement = entry.getValue().getOnHeapMemoryMb();
            totalMemoryRequired += memoryRequirement * parallelism;
        }
        return totalMemoryRequired;
    }

    public static Map<String, Integer> getComponentParallelism(Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        Map<String, Integer> ret = new HashMap<>();
        Map<String, Object> components = StormCommon.allComponents(topology);
        for (Map.Entry<String, Object> entry : components.entrySet()) {
            ret.put(entry.getKey(), getComponentParallelism(topoConf, entry.getValue()));
        }
        return ret;
    }

    public static int getComponentParallelism(Map<String, Object> topoConf, Object component) throws InvalidTopologyException {
        Map<String, Object> combinedConf = Utils.merge(topoConf, StormCommon.componentConf(component));
        int numTasks = ObjectReader.getInt(combinedConf.get(Config.TOPOLOGY_TASKS), StormCommon.numStartExecutors(component));
        Integer maxParallel = ObjectReader.getInt(combinedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM), null);
        int ret = numTasks;
        if (maxParallel != null) {
            ret = Math.min(maxParallel, numTasks);
        }
        return ret;
    }

    public static Subject principalNameToSubject(String name) {
        SingleUserPrincipal principal = new SingleUserPrincipal(name);
        Subject sub = new Subject();
        sub.getPrincipals().add(principal);
        return sub;
    }

    // Non-static impl methods exist for mocking purposes.
    public String currentClasspathImpl() {
        return System.getProperty("java.class.path");
    }

    public void extractDirFromJarImpl(String jarpath, String dir, File destdir) {
        try (JarFile jarFile = new JarFile(jarpath)) {
            extractZipFile(jarFile, destdir, dir);
        } catch (IOException e) {
            LOG.info("Could not extract {} from {}", dir, jarpath);
        }
    }

    public void downloadResourcesAsSupervisorImpl(String key, String localFile,
                                                  ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException, IOException {
        final int MAX_RETRY_ATTEMPTS = 2;
        final int ATTEMPTS_INTERVAL_TIME = 100;
        for (int retryAttempts = 0; retryAttempts < MAX_RETRY_ATTEMPTS; retryAttempts++) {
            if (downloadResourcesAsSupervisorAttempt(cb, key, localFile)) {
                break;
            }
            Utils.sleep(ATTEMPTS_INTERVAL_TIME);
        }
    }
}
