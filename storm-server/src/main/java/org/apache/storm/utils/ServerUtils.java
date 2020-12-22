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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.blobstore.LocalModeClientBlobStore;
import org.apache.storm.daemon.Acker;
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
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
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

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf,
            NimbusInfo nimbusInfo,
            ILeaderElector leaderElector) {
        return getNimbusBlobStore(conf, null, nimbusInfo, leaderElector);
    }

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf,
            String baseDir,
            NimbusInfo nimbusInfo,
            ILeaderElector leaderElector) {
        String type = (String) conf.get(DaemonConfig.NIMBUS_BLOBSTORE);
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
    public static long getDiskUsage(File dir) {
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
                        size += getDiskUsage(allFiles[i]);
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
     * Returns the value of java.class.path System property. Kept separate for testing.
     *
     * @return the classpath
     */
    public static String currentClasspath() {
        return _instance.currentClasspathImpl();
    }


    /**
     *  Returns the current thread classloader.
     */
    public static URL getResourceFromClassloader(String name) {
        return _instance.getResourceFromClassloaderImpl(name);
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
        return writeScript(dir, command, environment, null);
    }

    /**
     * Writes a posix shell script file to be executed in its own process.
     *
     * @param dir         the directory under which the script is to be written
     * @param command     the command the script is to execute
     * @param environment optional environment variables to set before running the script's command. May be  null.
     * @param umask umask to be set. It can be null.
     * @return the path to the script that has been written
     */
    public static String writeScript(String dir, List<String> command,
                                     Map<String, String> environment, String umask) throws IOException {
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
            if (umask != null) {
                out.write("umask " + umask);
                out.newLine();
            }
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
            throw new IOException("Mkdirs failed to create " + dir.toString());
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input
     * @param untarDir The untar directory where to untar the tar file
     * @param symlinksDisabled true if symlinks should be disabled, else false
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
            throw new IOException("Error untarring file "
                    + inFile
                    + ". Tar process exited with exit code "
                    + exitcode);
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
        if (lowerDst.endsWith(".jar")
                || lowerDst.endsWith("_jar")) {
            unJar(localrsrc, dst);
        } else if (lowerDst.endsWith(".zip")
                || lowerDst.endsWith("_zip")) {
            unZip(localrsrc, dst);
        } else if (lowerDst.endsWith(".tar.gz")
                || lowerDst.endsWith("_tar_gz")
                || lowerDst.endsWith(".tgz")
                || lowerDst.endsWith("_tgz")
                || lowerDst.endsWith(".tar")
                || lowerDst.endsWith("_tar")) {
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
    
    /**
     * Extracts the given file to the given directory. Only zip entries starting with the given prefix are extracted.
     * The prefix is stripped off entry names before extraction.
     *
     * @param zipFile The zip file to extract
     * @param toDir The directory to extract to
     * @param prefix The prefix to look for in the zip file. If not null only paths starting with the prefix will be
     *     extracted
     */
    public static void extractZipFile(ZipFile zipFile, File toDir, String prefix) throws IOException {
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
                String entryName;
                if (prefix != null) {
                    entryName = entry.getName().substring(prefix.length());
                    LOG.debug("Extracting {} shortened to {} into {}", entry.getName(), entryName, toDir);
                } else {
                    entryName = entry.getName();
                }
                File file = new File(toDir, entryName);
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
     * @param toDir The unzip directory where to unzip the zip file
     */
    public static void unZip(File inFile, File toDir) throws IOException {
        try (ZipFile zipFile = new ZipFile(inFile)) {
            extractZipFile(zipFile, toDir, null);
        }
    }

    /**
     * Given a zip File input it will return its size Only works for zip files whose uncompressed size is less than 4 GB, otherwise returns
     * the size module 2^32, per gzip specifications.
     *
     * @param myFile The zip file as input
     * @return zip file size as a long
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

    /**
     * Check if the scheduler is resource aware or not.
     *
     * @param conf The configuration
     * @return True if it's resource aware; false otherwise
     */
    public static boolean isRas(Map<String, Object> conf) {
        if (conf.containsKey(DaemonConfig.STORM_SCHEDULER)) {
            if (conf.get(DaemonConfig.STORM_SCHEDULER).equals("org.apache.storm.scheduler.resource.ResourceAwareScheduler")) {
                return true;
            }
        }
        return false;
    }

    public static int getEstimatedWorkerCountForRasTopo(Map<String, Object> topoConf, StormTopology topology)
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


    public URL getResourceFromClassloaderImpl(String name) {
        return Thread.currentThread().getContextClassLoader().getResource(name);
    }

    private static final Pattern MEMINFO_PATTERN = Pattern.compile("^([^:\\s]+):\\s*([0-9]+)\\s*kB$");

    /**
     * Get system free memory in megabytes.
     * @return system free memory in megabytes
     * @throws IOException on I/O exception
     */
    public static long getMemInfoFreeMb() throws IOException {
        //MemFree:        14367072 kB
        //Buffers:          536512 kB
        //Cached:          1192096 kB
        // MemFree + Buffers + Cached
        long memFree = 0;
        long buffers = 0;
        long cached = 0;
        try (BufferedReader in = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line = null;
            while ((line = in.readLine()) != null) {
                Matcher match = MEMINFO_PATTERN.matcher(line);
                if (match.matches()) {
                    String tag = match.group(1);
                    if (tag.equalsIgnoreCase("MemFree")) {
                        memFree = Long.parseLong(match.group(2));
                    } else if (tag.equalsIgnoreCase("Buffers")) {
                        buffers = Long.parseLong(match.group(2));
                    } else if (tag.equalsIgnoreCase("Cached")) {
                        cached = Long.parseLong(match.group(2));
                    }
                }
            }
        }
        return (memFree + buffers + cached) / 1024;
    }

    /**
     * Is a process alive and running?.
     *
     * @param pid the PID of the running process
     * @param user the user that is expected to own that process
     * @return true if it is, else false
     *
     * @throws IOException on any error
     */
    public static boolean isProcessAlive(long pid, String user) throws IOException {
        if (ServerUtils.IS_ON_WINDOWS) {
            return isWindowsProcessAlive(pid, user);
        }
        return isPosixProcessAlive(pid, user);
    }

    private static boolean isWindowsProcessAlive(long pid, String user) throws IOException {
        boolean ret = false;
        LOG.debug("CMD: tasklist /fo list /fi \"pid eq {}\" /v", pid);
        ProcessBuilder pb = new ProcessBuilder("tasklist", "/fo", "list", "/fi", "pid eq " + pid, "/v");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            int lineNo = 0;
            String line;
            while ((line = in.readLine()) != null) {
                lineNo++;
                LOG.debug("CMD=LINE#{}: {}", lineNo, line);
                if (line.contains("User Name:")) { //Check for : in case someone called their user "User Name"
                    //This line contains the user name for the pid we're looking up
                    //Example line: "User Name:    exampleDomain\exampleUser"
                    List<String> userNameLineSplitOnWhitespace = Arrays.asList(line.split(":"));
                    if (userNameLineSplitOnWhitespace.size() == 2) {
                        List<String> userAndMaybeDomain = Arrays.asList(userNameLineSplitOnWhitespace.get(1).trim().split("\\\\"));
                        String processUser = userAndMaybeDomain.size() == 2 ? userAndMaybeDomain.get(1) : userAndMaybeDomain.get(0);
                        processUser = processUser.trim();
                        if (user.equals(processUser)) {
                            ret = true;
                        } else {
                            LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                        }
                    } else {
                        LOG.error("Received unexpected output from tasklist command. Expected one colon in user name line. Line was {}",
                            line);
                    }
                    break;
                }
            }
        }
        return ret;
    }

    private static boolean isPosixProcessAlive(long pid, String user) throws IOException {
        LOG.debug("CMD: ps -o user -p {}", pid);
        ProcessBuilder pb = new ProcessBuilder("ps", "-o", "user", "-p", String.valueOf(pid));
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            int lineNo = 1;
            String line = in.readLine();
            LOG.debug("CMD-LINE#{}: {}", lineNo, line);
            if (!"USER".equals(line.trim())) {
                LOG.error("Expecting first line to contain USER, found \"{}\"", line);
                return false;
            }
            while ((line = in.readLine()) != null) {
                lineNo++;
                LOG.debug("CMD-LINE#{}: {}", lineNo, line);
                line = line.trim();
                if (user.equals(line)) {
                    return true;
                }
                LOG.info("Found {} running as {}, but expected it to be {}", pid, line, user);
            }
        } catch (IOException ex) {
            String err = String.format("Cannot read output of command \"ps -o user -p %d\"", pid);
            throw new IOException(err, ex);
        }
        return false;
    }

    /**
     * Are any of the processes alive and running for the specified user. If collection is empty or null
     * then the return value is trivially false.
     *
     * @param pids the PIDs of the running processes
     * @param user the user that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    public static boolean isAnyProcessAlive(Collection<Long> pids, String user) throws IOException {
        if (pids == null || pids.isEmpty()) {
            return false;
        }

        if (ServerUtils.IS_ON_WINDOWS) {
            return isAnyWindowsProcessAlive(pids, user);
        }
        return isAnyPosixProcessAlive(pids, user);
    }

    /**
     * Are any of the processes alive and running for the specified userId. If collection is empty or null
     * then the return value is trivially false.
     *
     * @param pids the PIDs of the running processes
     * @param uid the user that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    public static boolean isAnyProcessAlive(Collection<Long> pids, int uid) throws IOException {
        if (pids == null || pids.isEmpty()) {
            return false;
        }
        if (ServerUtils.IS_ON_WINDOWS) {
            return isAnyWindowsProcessAlive(pids, uid);
        }
        return isAnyPosixProcessAlive(pids, uid);
    }

    /**
     * Find if any of the Windows processes are alive and owned by the specified user.
     * Command reference https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/tasklist.
     *
     * @param pids the PIDs of the running processes
     * @param user the user that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    private static boolean isAnyWindowsProcessAlive(Collection<Long> pids, String user) throws IOException {
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add("tasklist");
        cmdArgs.add("/fo");
        cmdArgs.add("list");
        pids.forEach(pid -> {
            cmdArgs.add("/fi");
            cmdArgs.add("pid eq " + pid);
        });
        cmdArgs.add("/v");
        LOG.debug("CMD: {}", String.join(" ", cmdArgs));
        ProcessBuilder pb = new ProcessBuilder(cmdArgs);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        List<String> unexpectedUsers = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            int lineNo = 0;
            String line;
            while ((line = in.readLine()) != null) {
                lineNo++;
                LOG.debug("CMD-LINE#{}: {}", lineNo, line);
                if (line.contains("User Name:")) { //Check for : in case someone called their user "User Name"
                    //This line contains the user name for the pid we're looking up
                    //Example line: "User Name:    exampleDomain\exampleUser"
                    List<String> userNameLineSplitOnWhitespace = Arrays.asList(line.split(":"));
                    if (userNameLineSplitOnWhitespace.size() == 2) {
                        List<String> userAndMaybeDomain = Arrays.asList(userNameLineSplitOnWhitespace.get(1).trim().split("\\\\"));
                        String processUser = userAndMaybeDomain.size() == 2 ? userAndMaybeDomain.get(1) : userAndMaybeDomain.get(0);
                        processUser = processUser.trim();
                        if (user.equals(processUser)) {
                            return true;
                        }
                        unexpectedUsers.add(processUser);
                    } else {
                        LOG.error("Received unexpected output from tasklist command. Expected one colon in user name line. Line was {}",
                            line);
                    }
                }
            }
        } catch (IOException ex) {
            String err = String.format("Cannot read output of command \"%s\"", String.join(" ", cmdArgs));
            throw new IOException(err, ex);
        }
        String pidsAsStr = StringUtils.join(pids, ",");
        if (unexpectedUsers.isEmpty()) {
            LOG.info("None of the processes {} are alive", pidsAsStr);
        } else {
            LOG.info("{} of the Processes {} are running as user(s) {}: but expected user is {}",
                unexpectedUsers.size(), pidsAsStr, String.join(",", new TreeSet<>(unexpectedUsers)), user);
        }
        return false;
    }

    /**
     * Find if any of the Windows processes are alive and owned by the specified userId.
     * This overridden method is provided for symmetry, but is not implemented.
     *
     * @param pids the PIDs of the running processes
     * @param uid the user that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    private static boolean isAnyWindowsProcessAlive(Collection<Long> pids, int uid) throws IOException {
        throw new IllegalArgumentException("UID is not supported on Windows");
    }

    /**
     * Are any of the processes alive and running for the specified user.
     *
     * @param pids the PIDs of the running processes
     * @param user the user that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    private static boolean isAnyPosixProcessAlive(Collection<Long> pids, String user) throws IOException {
        String pidParams = StringUtils.join(pids, ",");
        LOG.debug("CMD: ps -o user -p {}", pidParams);
        ProcessBuilder pb = new ProcessBuilder("ps", "-o", "user", "-p", pidParams);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        List<String> unexpectedUsers = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            int lineNo = 1;
            String line = in.readLine();
            LOG.debug("CMD-LINE#{}: {}", lineNo, line);
            if (!"USER".equals(line.trim())) {
                LOG.error("Expecting first line to contain USER, found \"{}\"", line);
                return false;
            }
            while ((line = in.readLine()) != null) {
                lineNo++;
                LOG.debug("CMD-LINE#{}: {}", lineNo, line);
                line = line.trim();
                if (user.equals(line)) {
                    return true;
                }
                unexpectedUsers.add(line);
            }
        } catch (IOException ex) {
            String err = String.format("Cannot read output of command \"ps -o user -p %s\"", pidParams);
            throw new IOException(err, ex);
        }
        if (unexpectedUsers.isEmpty()) {
            LOG.info("None of the processes {} are alive", pidParams);
        } else {
            LOG.info("{} of {} Processes {} are running as user(s) {}: but expected user is {}",
                unexpectedUsers.size(), pids.size(), pidParams, String.join(",", new TreeSet<>(unexpectedUsers)), user);
        }
        return false;
    }

    /**
     * Are any of the processes alive and running for the specified UID.
     *
     * @param pids the PIDs of the running processes
     * @param uid the userId that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    private static boolean isAnyPosixProcessAlive(Collection<Long> pids, int uid) throws IOException {
        String pidParams = StringUtils.join(pids, ",");
        LOG.debug("CMD: ps -o uid -p {}", pidParams);
        ProcessBuilder pb = new ProcessBuilder("ps", "-o", "uid", "-p", pidParams);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        List<String> unexpectedUsers = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            int lineNo = 1;
            String line = in.readLine();
            LOG.debug("CMD-LINE#{}: {}", lineNo, line);
            if (!"UID".equals(line.trim())) {
                LOG.error("Expecting first line to contain UID, found \"{}\"", line);
                return false;
            }
            while ((line = in.readLine()) != null) {
                lineNo++;
                LOG.debug("CMD-LINE#{}: {}", lineNo, line);
                line = line.trim();
                try {
                    if (uid == Integer.parseInt(line)) {
                        return true;
                    }
                } catch (Exception ex) {
                    LOG.warn("Expecting UID integer but got {} in output of ps command", line);
                }
                unexpectedUsers.add(line);
            }
        } catch (IOException ex) {
            String err = String.format("Cannot read output of command \"ps -o uid -p %s\"", pidParams);
            throw new IOException(err, ex);
        }
        if (unexpectedUsers.isEmpty()) {
            LOG.info("None of the processes {} are alive", pidParams);
        } else {
            LOG.info("{} of {} Processes {} are running as UIDs {}: but expected userId is {}",
                unexpectedUsers.size(), pids.size(), pidParams, String.join(",", new TreeSet<>(unexpectedUsers)), uid);
        }
        return false;
    }

    /**
     * Get the userId for a user name. This works on Posix systems by using "id -u" command.
     * Throw IllegalArgumentException on Windows.
     *
     * @param user username to be converted to UID. This is optional, in which case current user is returned.
     * @return UID for the specified user (if supplied), else UID of current user, -1 upon Exception.
     */
    public static int getUserId(String user) {
        if (ServerUtils.IS_ON_WINDOWS) {
            throw new IllegalArgumentException("Not supported in Windows platform");
        }
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add("id");
        cmdArgs.add("-u");
        if (user != null && !user.isEmpty()) {
            cmdArgs.add(user);
        }
        LOG.debug("CMD: {}", String.join(" ", cmdArgs));
        ProcessBuilder pb = new ProcessBuilder(cmdArgs);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            String line = in.readLine();
            LOG.debug("CMD-LINE#1: {}", line);
            try {
                return Integer.parseInt(line.trim());
            } catch (NumberFormatException ex) {
                LOG.error("Expecting UID integer but got {} in output of \"id -u {}\" command", line, user);
                return -1;
            }
        } catch (IOException ex) {
            LOG.error(String.format("Cannot read output of command \"%s\"", String.join(" ", cmdArgs)), ex);
            return -1;
        }
    }

    /**
     * Get the userId of the onwer of the path by running "ls -dn path" command.
     * This command works on Posix systems only.
     *
     * @param fpath full path to the file or directory.
     * @return UID for the specified if successful, -1 upon failure.
     */
    public static int getPathOwnerUid(String fpath) {
        if (ServerUtils.IS_ON_WINDOWS) {
            throw new IllegalArgumentException("Not supported in Windows platform");
        }
        File f = new File(fpath);
        if (!f.exists()) {
            LOG.error("Cannot determine owner of non-existent file {}", fpath);
            return -1;
        }
        LOG.debug("CMD: ls -dn {}", fpath);
        ProcessBuilder pb = new ProcessBuilder("ls", "-dn", fpath);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(pb.start().getInputStream()))) {
            String line = in.readLine();
            LOG.debug("CMD-OUTLINE: {}", line);
            line = line.trim();
            String[] parts = line.split("\\s+");
            if (parts.length < 3) {
                LOG.error("Expecting at least 3 space separated fields in \"ls -dn {}\" output, got {}", fpath, line);
                return -1;
            }
            try {
                return Integer.parseInt(parts[2]);
            } catch (NumberFormatException ex) {
                LOG.error("Expecting at third field {} to be numeric UID \"ls -dn {}\" output, got {}", parts[2], fpath, line);
                return -1;
            }
        } catch (IOException ex) {
            LOG.error(String.format("Cannot read output of command \"ls -dn %s\"", fpath), ex);
            return -1;
        }
    }

    /**
     * Get UID of the owner to the workerId Root directory.
     *
     * @return User ID (UID) of owner of the workerId root directory, -1 if directory is missing.
     */
    private static int getWorkerPathOwnerUid(Map<String, Object> conf, String workerId) {
        return getPathOwnerUid(ConfigUtils.workerRoot(conf, workerId));
    }

    private static final Map<String, Integer> cachedUserToUidMap = new ConcurrentHashMap<>();

    /**
     * Find if all processes for the user on workId are dead.
     * This method attempts to optimize the calls by:
     * <p>
     *     <li>checking a collection of ProcessIds at once</li>
     *     <li>using userId one Posix systems instead of user</li>
     * </p>
     *
     * @return true if all processes for the user are dead on the worker
     * @throws IOException if external commands have exception.
     */
    public static boolean areAllProcessesDead(Map<String, Object> conf, String user, String workerId, Set<Long> pids) throws IOException {
        if (pids == null || pids.isEmpty()) {
            return true;
        }

        if (ServerUtils.IS_ON_WINDOWS) {
            return !isAnyProcessAlive(pids, user);
        }
        // optimized for Posix - first examine /proc and then use optimized version of "ps" with uid
        try {
            return !isAnyPosixProcessPidDirAlive(pids, user);
        } catch (IOException ex) {
            LOG.warn("Failed to determine if processes {} for user {} are dead using filesystem, will try \"ps\" command: {}",
                    pids, user, ex);
        }
        if (!cachedUserToUidMap.containsKey(user)) {
            int uid = ServerUtils.getWorkerPathOwnerUid(conf, workerId);
            if (uid < 0) {
                uid = ServerUtils.getUserId(user);
            }
            if (uid >= 0) {
                cachedUserToUidMap.put(user, uid);
            }
        }
        if (cachedUserToUidMap.containsKey(user)) {
            return !ServerUtils.isAnyProcessAlive(pids, cachedUserToUidMap.get(user));
        } else {
            return !ServerUtils.isAnyProcessAlive(pids, user);
        }
    }

    /**
     * Find if the process is alive using the existence of /proc/&lt;pid&gt; directory
     * owned by the supplied user. This is an alternative to "ps -p pid -u uid" command
     * used in {@link #isAnyPosixProcessAlive(Collection, int)}
     *
     * <p>
     * Processes are tracked using the existence of the directory "/proc/&lt;pid&gt;
     * For each of the supplied PIDs, their PID directory is checked for existence and ownership
     * by the specified uid.
     * </p>
     *
     * @param pids Process IDs that need to be monitored for liveness
     * @param user the userId that is expected to own that process
     * @return true if any one of the processes is owned by user and alive, else false
     * @throws IOException on I/O exception
     */
    public static boolean isAnyPosixProcessPidDirAlive(Collection<Long> pids, String user) throws IOException {
        return isAnyPosixProcessPidDirAlive(pids, user, false);
    }

    /**
     * Find if the process is alive using the existence of /proc/&lt;pid&gt; directory
     * owned by the supplied expectedUser. This is an alternative to "ps -p pid -u uid" command
     * used in {@link #isAnyPosixProcessAlive(Collection, int)}
     *
     * <p>
     * Processes are tracked using the existence of the directory "/proc/&lt;pid&gt;
     * For each of the supplied PIDs, their PID directory is checked for existence and ownership
     * by the specified uid.
     * </p>
     *
     * @param pids Process IDs that need to be monitored for liveness
     * @param expectedUser the userId that is expected to own that process
     * @param mockFileOwnerToUid if true (used for testing), then convert File.owner to UID
     * @return true if any one of the processes is owned by expectedUser and alive, else false
     * @throws IOException on I/O exception
     */
    @VisibleForTesting
    public static boolean isAnyPosixProcessPidDirAlive(Collection<Long> pids, String expectedUser, boolean mockFileOwnerToUid)
            throws IOException {
        File procDir = new File("/proc");
        if (!procDir.exists()) {
            throw new IOException("Missing process directory " + procDir.getAbsolutePath() + ": method not supported on "
                    + "os.name=" + System.getProperty("os.name"));
        }
        for (long pid: pids) {
            File pidDir = new File(procDir, String.valueOf(pid));
            if (!pidDir.exists()) {
                continue;
            }
            // check if existing process is owned by the specified expectedUser, if not, the process is dead
            String actualUser;
            try {
                actualUser = Files.getOwner(pidDir.toPath()).getName();
            } catch (NoSuchFileException ex) {
                continue; // process died before the expectedUser can be checked
            }
            if (mockFileOwnerToUid) {
                // code activated in testing to simulate Files.getOwner returning UID (which sometimes happens in runtime)
                if (StringUtils.isNumeric(actualUser)) {
                    LOG.info("Skip mocking, since owner {} of pidDir {} is already numeric", actualUser, pidDir);
                } else {
                    Integer actualUid = cachedUserToUidMap.get(actualUser);
                    if (actualUid == null) {
                        actualUid = ServerUtils.getUserId(actualUser);
                        if (actualUid < 0) {
                            String err = String.format("Cannot get UID for %s, while mocking the owner of pidDir %s",
                                    actualUser, pidDir.getAbsolutePath());
                            throw new IOException(err);
                        }
                        cachedUserToUidMap.put(actualUser, actualUid);
                        LOG.info("Found UID {} for {}, while mocking the owner of pidDir {}", actualUid, actualUser, pidDir);
                    } else {
                        LOG.info("Found cached UID {} for {}, while mocking the owner of pidDir {}", actualUid, actualUser, pidDir);
                    }
                    actualUser = String.valueOf(actualUid);
                }
            }
            //sometimes uid is returned instead of username - if so, try to convert and compare with uid
            if (StringUtils.isNumeric(actualUser)) {
                // numeric actualUser - this is UID not user
                LOG.debug("Process directory {} owner is uid={}", pidDir, actualUser);
                int actualUid = Integer.parseInt(actualUser);
                Integer expectedUid = cachedUserToUidMap.get(expectedUser);
                if (expectedUid == null) {
                    expectedUid = ServerUtils.getUserId(expectedUser);
                    if (expectedUid < 0) {
                        String err = String.format("Cannot get uid for %s to compare with owner id=%d of process directory %s",
                                expectedUser, actualUid, pidDir.getAbsolutePath());
                        throw new IOException(err);
                    }
                    cachedUserToUidMap.put(expectedUser, expectedUid);
                }
                if (expectedUid == actualUid) {
                    LOG.debug("Process {} is alive and owned by expectedUser {}/{}", pid, expectedUser, expectedUid);
                    return true;
                }
                LOG.info("Prior process is dead, since directory {} owner {} is not same as expectedUser {}/{}, "
                        + "likely pid {} was reused for a new process for uid {}, {}",
                        pidDir, actualUser, expectedUser, expectedUid, pid, actualUid, getProcessDesc(pidDir));
            } else {
                // actualUser is a string
                LOG.debug("Process directory {} owner is {}", pidDir, actualUser);
                if (expectedUser.equals(actualUser)) {
                    LOG.debug("Process {} is alive and owned by expectedUser {}", pid, expectedUser);
                    return true;
                }
                LOG.info("Prior process is dead, since directory {} owner {} is not same as expectedUser {}, "
                        + "likely pid {} was reused for a new process for actualUser {}, {}}",
                        pidDir, actualUser, expectedUser, pid, actualUser, getProcessDesc(pidDir));
            }
        }
        LOG.info("None of the processes {} are alive AND owned by expectedUser {}", pids, expectedUser);
        return false;
    }

    @VisibleForTesting
    public static void validateTopologyWorkerMaxHeapSizeConfigs(
        Map<String, Object> stormConf, StormTopology topology, double defaultWorkerMaxHeapSizeMb)
        throws InvalidTopologyException {

        double largestMemReq = getMaxExecutorMemoryUsageForTopo(topology, stormConf);
        double topologyWorkerMaxHeapSize =
            ObjectReader.getDouble(stormConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB), defaultWorkerMaxHeapSizeMb);
        if (topologyWorkerMaxHeapSize < largestMemReq) {
            throw new InvalidTopologyException(
                "Topology will not be able to be successfully scheduled: Config "
                    + "TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB="
                    + topologyWorkerMaxHeapSize
                    + " < " + largestMemReq + " (Largest memory requirement of a component in the topology)."
                    + " Perhaps set TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB to a larger amount");
        }
    }

    /**
     * RAS scheduler will try to distribute ackers evenly over workers by adding some ackers to each newly launched worker.
     * Validations are performed here:
     * ({@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER} * memory for an acker
     *      + memory for the biggest topo executor) < max worker heap memory.
     *    When RAS tries to schedule an executor to a new worker,
     *    it will put {@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER} ackers into the worker first.
     *    So {@link Config#TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB} need to be able to accommodate this.
     * @param topoConf Topology conf
     * @param topology Topology (not system topology)
     * @param topoName The name of the topology
     */
    public static void validateTopologyAckerBundleResource(Map<String, Object> topoConf,
                                                           StormTopology topology, String topoName)
        throws InvalidTopologyException {

        boolean oneExecutorPerWorker = (Boolean) topoConf.getOrDefault(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, false);
        boolean oneComponentPerWorker = (Boolean) topoConf.getOrDefault(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER, false);

        double topologyWorkerMaxHeapSize =
            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB));

        int numOfAckerExecutorsPerWorker = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER));
        double maxTopoExecMem = getMaxExecutorMemoryUsageForTopo(topology, topoConf);
        double ackerExecMem = getAckerExecutorMemoryUsageForTopo(topology, topoConf);
        double minMemReqForWorker = maxTopoExecMem + ackerExecMem * numOfAckerExecutorsPerWorker;

        // A worker need to have enough resources for a bigest topo executor + topology.acker.executors.per.worker ackers
        if (!oneExecutorPerWorker
            && !oneComponentPerWorker
            && topologyWorkerMaxHeapSize < minMemReqForWorker) {
            String warnMsg
                = String.format("For topology %s. Worker max on-heap limit %s is %s. "
                    + "The biggest topo executor requires %s MB on-heap memory, "
                    + "there might not be enough space for %s ackers. "
                    + "Real acker-per-worker will be determined by scheduler.",
                    topoName, Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, topologyWorkerMaxHeapSize,
                    maxTopoExecMem, numOfAckerExecutorsPerWorker);
            LOG.warn(warnMsg);
        }
    }

    private static double getMaxExecutorMemoryUsageForTopo(
        StormTopology topology, Map<String, Object> topologyConf) {
        double largestMemoryOperator = 0.0;
        for (NormalizedResourceRequest entry :
            ResourceUtils.getBoltsResources(topology, topologyConf).values()) {
            double memoryRequirement = entry.getTotalMemoryMb();
            if (memoryRequirement > largestMemoryOperator) {
                largestMemoryOperator = memoryRequirement;
            }
        }
        for (NormalizedResourceRequest entry :
            ResourceUtils.getSpoutsResources(topology, topologyConf).values()) {
            double memoryRequirement = entry.getTotalMemoryMb();
            if (memoryRequirement > largestMemoryOperator) {
                largestMemoryOperator = memoryRequirement;
            }
        }
        return largestMemoryOperator;
    }

    private static double getAckerExecutorMemoryUsageForTopo(
        StormTopology topology, Map<String, Object> topologyConf)
        throws InvalidTopologyException {
        topology = StormCommon.systemTopology(topologyConf, topology);
        Map<String, NormalizedResourceRequest> boltResources = ResourceUtils.getBoltsResources(topology, topologyConf);
        NormalizedResourceRequest entry = boltResources.get(Acker.ACKER_COMPONENT_ID);
        return entry.getTotalMemoryMb();
    }

    /**
     * Support method to obtain additional log info for the process. Use the contents of comm and cmdline
     * in the process directory. Note that this method works properly only on posix systems with /proc directory.
     *
     * @param pidDir PID directory (/proc/&lt;pid&gt;)
     * @return process description string
     */
    private static String getProcessDesc(File pidDir) {
        String comm = "";
        Path p = pidDir.toPath().resolve("comm");
        try {
            comm = String.join(", ", Files.readAllLines(p));
        } catch (IOException ex) {
            LOG.warn("Cannot get contents of " + p, ex);
        }
        String cmdline = "";
        p = pidDir.toPath().resolve("cmdline");
        try {
            cmdline = String.join(", ", Files.readAllLines(p)).replace('\0', ' ');
        } catch (IOException ex) {
            LOG.warn("Cannot get contents of " + p, ex);
        }
        return String.format("process(comm=\"%s\", cmdline=\"%s\")", comm, cmdline);
    }
}
