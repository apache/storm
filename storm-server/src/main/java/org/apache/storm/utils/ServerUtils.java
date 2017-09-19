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
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.*;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.thrift.TException;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ServerUtils {
    public static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);

    public static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
    public static final String CLASS_PATH_SEPARATOR = System.getProperty("path.separator");
    public static final boolean IS_ON_WINDOWS = "Windows_NT".equals(System.getenv("OS"));
    public static final String CURRENT_BLOB_SUFFIX_ID = "current";

    public static final String DEFAULT_CURRENT_BLOB_SUFFIX = "." + CURRENT_BLOB_SUFFIX_ID;
    public static final String DEFAULT_BLOB_VERSION_SUFFIX = ".version";
    public static final int SIGKILL = 9;
    public static final int SIGTERM = 15;
    /**
     * Make sure a given key name is valid for the storm config.
     * Throw RuntimeException if the key isn't valid.
     * @param name The name of the config key to check.
     */
    private static final Set<String> disallowedKeys = new HashSet<>(Arrays.asList(new String[] {"/", ".", ":", "\\"}));

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ServerUtils _instance = new ServerUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
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

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf, NimbusInfo nimbusInfo) {
        return getNimbusBlobStore(conf, null, nimbusInfo);
    }

    public static BlobStore getNimbusBlobStore(Map<String, Object> conf, String baseDir, NimbusInfo nimbusInfo) {
        String type = (String)conf.get(DaemonConfig.NIMBUS_BLOBSTORE);
        if (type == null) {
            type = LocalFsBlobStore.class.getName();
        }
        BlobStore store = (BlobStore) ReflectionUtils.newInstance(type);
        HashMap nconf = new HashMap(conf);
        // only enable cleanup of blobstore on nimbus
        nconf.put(Config.BLOBSTORE_CLEANUP_ENABLE, Boolean.TRUE);

        if(store != null) {
            // store can be null during testing when mocking utils.
            store.prepare(nconf, baseDir, nimbusInfo);
        }
        return store;
    }

    public static boolean isAbsolutePath(String path) {
        return Paths.get(path).isAbsolute();
    }

    /**
     * Returns the combined string, escaped for posix shell.
     * @param command the list of strings to be combined
     * @return the resulting command string
     */
    public static String shellCmd (List<String> command) {
        List<String> changedCommands = new ArrayList<>(command.size());
        for (String str: command) {
            if (str == null) {
                continue;
            }
            changedCommands.add("'" + str.replaceAll("'", "'\"'\"'") + "'");
        }
        return StringUtils.join(changedCommands, " ");
    }

    public static String constructVersionFileName(String fileName) {
        return fileName + DEFAULT_BLOB_VERSION_SUFFIX;
    }

    public static String constructBlobCurrentSymlinkName(String fileName) {
        return fileName + DEFAULT_CURRENT_BLOB_SUFFIX;
    }

    /**
     * Takes an input dir or file and returns the disk usage on that local directory.
     * Very basic implementation.
     *
     * @param dir The input dir to get the disk space of this local dir
     * @return The total disk space of the input local directory
     */
    public static long getDU(File dir) {
        long size = 0;
        if (!dir.exists())
            return 0;
        if (!dir.isDirectory()) {
            return dir.length();
        } else {
            File[] allFiles = dir.listFiles();
            if(allFiles != null) {
                for (int i = 0; i < allFiles.length; i++) {
                    boolean isSymLink;
                    try {
                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
                    } catch(IOException ioe) {
                        isSymLink = true;
                    }
                    if(!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }
            return size;
        }
    }

    public static long localVersionOfBlob(String localFile) {
        return Utils.getVersionFromBlobVersionFile(new File(localFile + DEFAULT_BLOB_VERSION_SUFFIX));
    }

    public static String constructBlobWithVersionFileName(String fileName, long version) {
        return fileName + "." + version;
    }

    public static ClientBlobStore getClientBlobStoreForSupervisor(Map<String, Object> conf) {
        ClientBlobStore store = (ClientBlobStore) ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.SUPERVISOR_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
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
     * @param dir Directory in the jar to pull out
     * @param destdir Path to the directory where the extracted directory will be put
     */
    public static void extractDirFromJar(String jarpath, String dir, File destdir) {
        _instance.extractDirFromJarImpl(jarpath, dir, destdir);
    }

    /**
     * Returns the value of java.class.path System property. Kept separate for
     * testing.
     * @return the classpath
     */
    public static String currentClasspath() {
        return _instance.currentClasspathImpl();
    }

    /**
     * Determines if a zip archive contains a particular directory.
     *
     * @param zipfile path to the zipped file
     * @param target directory being looked for in the zip.
     * @return boolean whether or not the directory exists in the zip.
     */
    public static boolean zipDoesContainDir(String zipfile, String target) throws IOException {
        List<ZipEntry> entries = (List<ZipEntry>) Collections.list(new ZipFile(zipfile).entries());

        String targetDir = target + "/";
        for(ZipEntry entry : entries) {
            String name = entry.getName();
            if(name.startsWith(targetDir)) {
                return true;
            }
        }

        return false;
    }

    public static String getFileOwner(String path) throws IOException {
        return Files.getOwner(FileSystems.getDefault().getPath(path)).getName();
    }

    public static Localizer createLocalizer(Map<String, Object> conf, String baseDir) {
        return new Localizer(conf, baseDir);
    }

    public static String containerFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "launch_container.sh";
    }

    public static String scriptFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "storm-worker-script.sh";
    }

    /**
     * Writes a posix shell script file to be executed in its own process.
     * @param dir the directory under which the script is to be written
     * @param command the command the script is to execute
     * @param environment optional environment variables to set before running the script's command. May be  null.
     * @return the path to the script that has been written
     */
    public static String writeScript(String dir, List<String> command,
                                     Map<String,String> environment) throws IOException {
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
                                    "export",k+"="+v)));
                    out.write(";");
                    out.newLine();
                }
            }
            out.newLine();
            out.write("exec "+ shellCmd(command)+";");
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

    public static void killProcessWithSigTerm (String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGTERM);
    }

    public static void forceKillProcess (String pid) throws IOException {
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
     * Unpack matching files from a jar. Entries inside the jar that do
     * not match the given pattern will be skipped.
     *
     * @param jarFile the .jar file to unpack
     * @param toDir the destination directory into which to unpack the jar
     */
    public static void unJar(File jarFile, File toDir)
            throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = jar.getInputStream(entry);
                    try {
                        File file = new File(toDir, entry.getName());
                        ensureDirectory(file.getParentFile());
                        OutputStream out = new FileOutputStream(file);
                        try {
                            copyBytes(in, out, 8192);
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
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
     * Given a Tar File as input it will untar the file in a the untar directory
     * passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input.
     * @param untarDir The untar directory where to untar the tar file.
     * @throws IOException
     */
    public static void unTar(File inFile, File untarDir) throws IOException {
        if (!untarDir.mkdirs()) {
            if (!untarDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + untarDir);
            }
        }

        boolean gzipped = inFile.toString().endsWith("gz");
        if (Utils.isOnWindows()) {
            // Tar is not native to Windows. Use simple Java based implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, untarDir, gzipped);
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
        String[] shellCmd = {"bash", "-c", untarCommand.toString()};
        ShellUtils.ShellCommandExecutor shexec = new ShellUtils.ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile +
                    ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir,
                                       boolean gzipped) throws IOException {
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
                    unpackEntries(tis, entry, untarDir);
                    entry = tis.getNextTarEntry();
                }
            }
        } finally {
            if(inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir) throws IOException {
        if (entry.isDirectory()) {
            File subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdirs() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                        + outputDir);
            }
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, subDir);
            }
            return;
        }
        File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.getParentFile().exists()) {
            if (!outputFile.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                                      + outputDir);
            }
        }
        int count;
        byte data[] = new byte[2048];
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(outputFile));

        while ((count = tis.read(data)) != -1) {
            outputStream.write(data, 0, count);
        }
        outputStream.flush();
        outputStream.close();
    }

    public static void unpack(File localrsrc, File dst) throws IOException {
        String lowerDst = localrsrc.getName().toLowerCase();
        if (lowerDst.endsWith(".jar")) {
            unJar(localrsrc, dst);
        } else if (lowerDst.endsWith(".zip")) {
            unZip(localrsrc, dst);
        } else if (lowerDst.endsWith(".tar.gz") ||
                lowerDst.endsWith(".tgz") ||
                lowerDst.endsWith(".tar")) {
            unTar(localrsrc, dst);
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
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     * @param inFile The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException
     */
    public static void unZip(File inFile, File unzipDir) throws IOException {
        Enumeration<? extends ZipEntry> entries;
        ZipFile zipFile = new ZipFile(inFile);

        try {
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = zipFile.getInputStream(entry);
                    try {
                        File file = new File(unzipDir, entry.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " +
                                                      file.getParentFile().toString());
                            }
                        }
                        OutputStream out = new FileOutputStream(file);
                        try {
                            byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }
    }

    public static void validateKeyName(String name) {

        for(String key : disallowedKeys) {
            if( name.contains(key) ) {
                throw new RuntimeException("Key name cannot contain any of the following: " + disallowedKeys.toString());
            }
        }
        if(name.trim().isEmpty()) {
            throw new RuntimeException("Key name cannot be blank");
        }
    }

    /**
     * Given a zip File input it will return its size
     * Only works for zip files whose uncompressed size is less than 4 GB,
     * otherwise returns the size module 2^32, per gzip specifications
     * @param myFile The zip file as input
     * @throws IOException
     * @return zip file size as a long
     */
    public static long zipFileSize(File myFile) throws IOException{
        RandomAccessFile raf = new RandomAccessFile(myFile, "r");
        raf.seek(raf.length() - 4);
        long b4 = raf.read();
        long b3 = raf.read();
        long b2 = raf.read();
        long b1 = raf.read();
        long val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;
        raf.close();
        return val;
    }

    // Non-static impl methods exist for mocking purposes.
    public String currentClasspathImpl() {
        return System.getProperty("java.class.path");
    }

    public void extractDirFromJarImpl(String jarpath, String dir, File destdir) {
        try (JarFile jarFile = new JarFile(jarpath)) {
            Enumeration<JarEntry> jarEnums = jarFile.entries();
            while (jarEnums.hasMoreElements()) {
                JarEntry entry = jarEnums.nextElement();
                if (!entry.isDirectory() && entry.getName().startsWith(dir)) {
                    File aFile = new File(destdir, entry.getName());
                    aFile.getParentFile().mkdirs();
                    try (FileOutputStream out = new FileOutputStream(aFile);
                         InputStream in = jarFile.getInputStream(entry)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
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

    public static int getEstimatedWorkerCountForRASTopo(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        return (int) Math.ceil(getEstimatedTotalHeapMemoryRequiredByTopo(topoConf, topology) /
                ObjectReader.getDouble(topoConf.get(Config.WORKER_HEAP_MEMORY_MB)));
    }

    public static double getEstimatedTotalHeapMemoryRequiredByTopo(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        Map<String, Integer> componentParallelism = getComponentParallelism(topoConf, topology);
        double totalMemoryRequired = 0.0;

        for(Map.Entry<String, Map<String, Double>> entry: ResourceUtils.getBoltsResources(topology, topoConf).entrySet()) {
            int parallelism = componentParallelism.getOrDefault(entry.getKey(), 1);
            double memoryRequirement = entry.getValue().get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
            totalMemoryRequired += memoryRequirement * parallelism;
        }

        for(Map.Entry<String, Map<String, Double>> entry: ResourceUtils.getSpoutsResources(topology, topoConf).entrySet()) {
            int parallelism = componentParallelism.getOrDefault(entry.getKey(), 1);
            double memoryRequirement = entry.getValue().get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
            totalMemoryRequired += memoryRequirement * parallelism;
        }
        return totalMemoryRequired;
    }

    public static Map<String, Integer> getComponentParallelism(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
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
}
