/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);
    private static final String STORM_CORE_PROPERTIES_NAME = "storm-core-version-info.properties";
    private static final String STORM_CLIENT_PROPERTIES_NAME = "storm-client-version-info.properties";
    public static final IVersionInfo OUR_FULL_VERSION = new VersionInfoImpl("storm-client");
    public static final SimpleVersion OUR_VERSION = new SimpleVersion(OUR_FULL_VERSION.getVersion());

    private static class VersionInfoImpl implements IVersionInfo {
        private Properties info;

        protected VersionInfoImpl(String component) {
            info = new Properties();
            String versionInfoFile = component + "-version-info.properties";
            try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(versionInfoFile)) {
                if (is == null) {
                    throw new IOException("Resource not found");
                }
                info.load(is);
            } catch (IOException e) {
                LOG.error("Could not load {}", versionInfoFile, e);
            }
        }

        protected VersionInfoImpl(Properties info) {
            this.info = info;
        }

        @Override
        public String getVersion() {
            return info.getProperty("version", "Unknown");
        }

        @Override
        public String getRevision() {
            return info.getProperty("revision", "Unknown");
        }

        @Override
        public String getBranch() {
            return info.getProperty("branch", "Unknown");
        }

        @Override
        public String getDate() {
            return info.getProperty("date", "Unknown");
        }

        @Override
        public String getUser() {
            return info.getProperty("user", "Unknown");
        }

        @Override
        public String getUrl() {
            return info.getProperty("url", "Unknown");
        }

        @Override
        public String getSrcChecksum() {
            return info.getProperty("srcChecksum", "Unknown");
        }

        @Override
        public String getBuildVersion() {
            return this.getVersion()
                    + " from " + getRevision()
                    + " by " + getUser()
                    + " source checksum " + getSrcChecksum();
        }
    }

    /**
     * Look for the version of storm defined by the given classpath.
     * @param cp the classpath as a string to be parsed.
     * @return the IVersionInfo or null.
     */
    public static IVersionInfo getFromClasspath(String cp) {
        List<String> classpath = Arrays.asList(cp.split(File.pathSeparator));
        return getFromClasspath(classpath);
    }

    /**
     * Look for the version of storm defined by the given classpath.
     * @param classpath the classpath as list of files/directories.
     * @return the IVersionInfo or null.
     */
    public static IVersionInfo getFromClasspath(List<String> classpath) {
        IVersionInfo ret = getFromClasspath(classpath, STORM_CLIENT_PROPERTIES_NAME);
        if (ret == null) {
            //storm-core is needed here for backwards compatibility.
            ret = getFromClasspath(classpath, STORM_CORE_PROPERTIES_NAME);
        }
        return ret;
    }

    private static IVersionInfo getFromClasspath(List<String> classpath, final String propFileName) {
        IVersionInfo ret = null;
        for (String part: classpath) {
            Path p = Paths.get(part);
            if (Files.isDirectory(p)) {
                Path child = p.resolve(propFileName);
                if (Files.exists(child) && !Files.isDirectory(child)) {
                    try (FileReader reader = new FileReader(child.toFile())) {
                        Properties info = new Properties();
                        info.load(reader);
                        ret = new VersionInfoImpl(info);
                        break;
                    } catch (IOException e) {
                        LOG.error("Skipping {}; got an error while trying to parse the file.", part, e);
                    }
                }
            } else if (part.toLowerCase().endsWith(".jar")
                || part.toLowerCase().endsWith(".zip")) {
                //Treat it like a jar
                try (JarFile jf = new JarFile(p.toFile())) {
                    Enumeration<? extends ZipEntry> zipEnums = jf.entries();
                    while (zipEnums.hasMoreElements()) {
                        ZipEntry entry = zipEnums.nextElement();
                        if (!entry.isDirectory() && entry.getName().equals(propFileName)) {
                            try (InputStreamReader reader = new InputStreamReader(jf.getInputStream(entry))) {
                                Properties info = new Properties();
                                info.load(reader);
                                ret = new VersionInfoImpl(info);
                                break;
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Skipping {}; got an error while trying to parse the jar file.", part, e);
                }
            } else if (p.endsWith("*")) {
                //for a path like /<parent-path>/*
                try {
                    Path parent = p.getParent();
                    List<String> children = new ArrayList<>();
                    Files.list(parent)
                        //avoid infinite recursion
                        .filter(path -> !path.endsWith("*"))
                        .forEach(path -> children.add(path.toString()));
                    IVersionInfo resFromChildren = getFromClasspath(children, propFileName);
                    if (resFromChildren != null) {
                        ret = resFromChildren;
                        break;
                    }
                } catch (NullPointerException | IOException e) {
                    LOG.error("Skipping {}; got an error while trying to parse it", part, e);
                }
            } else {
                LOG.warn("Skipping {}; don't know what to do with it.", part);
            }
        }
        return ret;
    }

    /**
     * Get the version number of the build.
     * @return the version number of the build.
     */
    public static String getVersion() {
        return OUR_FULL_VERSION.getVersion();
    }

    /**
     * Get the SCM revision number of the build.
     * @return the SCM revision number of the build.
     */
    public static String getRevision() {
        return OUR_FULL_VERSION.getRevision();
    }

    /**
     * Get the SCM branch of the build.
     * @return the SCM branch of the build.
     */
    public static String getBranch() {
        return OUR_FULL_VERSION.getBranch();
    }

    /**
     * Get the date/time the build happened.
     * @return the date/time of the build.
     */
    public static String getDate() {
        return OUR_FULL_VERSION.getDate();
    }

    /**
     * Get the name of the user that did the build.
     * @return the name of the user that did the build.
     */
    public static String getUser() {
        return OUR_FULL_VERSION.getUser();
    }

    /**
     * Get the full SCM URL for the build.
     * @return the SCM URL of the build.
     */
    public static String getUrl() {
        return OUR_FULL_VERSION.getUrl();
    }

    /**
     * Get the checksum of the source.
     * @return the checksum of the source.
     */
    public static String getSrcChecksum() {
        return OUR_FULL_VERSION.getSrcChecksum();
    }

    /**
     * Get a descriptive representation of the build meant for human consumption.
     * @return a descriptive representation of the build.
     */
    public static String getBuildVersion() {
        return OUR_FULL_VERSION.getBuildVersion();
    }

    public static void main(String[] args) {
        System.out.println("Storm " + getVersion());
        System.out.println("URL " + getUrl() + " -r " + getRevision());
        System.out.println("Branch " + getBranch());
        System.out.println("Compiled by " + getUser() + " on " + getDate());
        System.out.println("From source with checksum " + getSrcChecksum());
    }
}
