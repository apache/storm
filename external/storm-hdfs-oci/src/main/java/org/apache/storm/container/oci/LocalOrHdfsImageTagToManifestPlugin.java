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

package org.apache.storm.container.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.HadoopLoginUtil;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalOrHdfsImageTagToManifestPlugin implements OciImageTagToManifestPluginInterface {
    private static final Logger LOG = LoggerFactory.getLogger(LocalOrHdfsImageTagToManifestPlugin.class);

    private Map<String, ImageManifest> manifestCache;
    private ObjectMapper objMapper;
    private Map<String, String> localImageToHashCache = new HashMap<>();
    private Map<String, String> hdfsImageToHashCache = new HashMap<>();
    private Map<String, Object> conf;
    private long hdfsModTime;
    private long localModTime;
    private String hdfsImageToHashFile;
    private String manifestDir;
    private String localImageTagToHashFile;
    private int ociCacheRefreshIntervalSecs;
    private long lastRefreshTime;

    private static final String LOCAL_OR_HDFS_IMAGE_TAG_TO_MANIFEST_PLUGIN_PREFIX = "storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.";

    /**
     * The HDFS location where the oci image-tag-to-hash file exists.
     */
    private static final String HDFS_OCI_IMAGE_TAG_TO_HASH_FILE =
        LOCAL_OR_HDFS_IMAGE_TAG_TO_MANIFEST_PLUGIN_PREFIX + "hdfs.hash.file";

    /**
     * The local file system location where the oci image-tag-to-hash file exists.
     */
    private static final String LOCAL_OCI_IMAGE_TAG_TO_HASH_FILE =
        LOCAL_OR_HDFS_IMAGE_TAG_TO_MANIFEST_PLUGIN_PREFIX + "local.hash.file";

    /**
     * The interval in seconds between refreshing the oci image-Tag-to-hash cache.
     */
    private static final String OCI_CACHE_REFRESH_INTERVAL =
        LOCAL_OR_HDFS_IMAGE_TAG_TO_MANIFEST_PLUGIN_PREFIX + "cache.refresh.interval.secs";

    /**
     * The number of manifests to cache.
     */
    private static final String OCI_NUM_MANIFESTS_TO_CACHE = LOCAL_OR_HDFS_IMAGE_TAG_TO_MANIFEST_PLUGIN_PREFIX + "num.manifests.to.cache";

    private static final int SHA256_HASH_LENGTH = 64;

    private static final String ALPHA_NUMERIC = "[a-zA-Z0-9]+";

    @Override
    public void init(Map<String, Object> conf) throws IOException {
        this.conf = conf;

        //login to hdfs
        HadoopLoginUtil.loginHadoop(conf);

        localImageTagToHashFile = (String) conf.get(LOCAL_OCI_IMAGE_TAG_TO_HASH_FILE);
        if (localImageTagToHashFile == null) {
            LOG.debug("Failed to load local oci-image-to-hash file. Config not set");
        }
        hdfsImageToHashFile = (String) conf.get(HDFS_OCI_IMAGE_TAG_TO_HASH_FILE);
        if (hdfsImageToHashFile == null) {
            LOG.debug("Failed to load HDFS oci-image-to-hash file. Config not set");
        }
        if (hdfsImageToHashFile == null && localImageTagToHashFile == null) {
            throw new IllegalArgumentException("No valid image-tag-to-hash files");
        }
        manifestDir = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_IMAGE_HDFS_TOPLEVEL_DIR)) + "/manifests/";
        int numManifestsToCache = ObjectReader.getInt(conf.get(OCI_NUM_MANIFESTS_TO_CACHE), 10);
        this.objMapper = new ObjectMapper();
        this.manifestCache = new LruCache(numManifestsToCache, 0.75f);
        ociCacheRefreshIntervalSecs = ObjectReader.getInt(conf.get(OCI_CACHE_REFRESH_INTERVAL), 60);
    }

    private boolean loadImageToHashFiles() throws IOException {
        boolean ret = false;
        try (BufferedReader localBr = getLocalImageToHashReader()) {
            Map<String, String> localImageToHash = readImageToHashFile(localBr, localImageTagToHashFile);
            if (localImageToHash != null && !localImageToHash.equals(localImageToHashCache)) {
                localImageToHashCache = localImageToHash;
                LOG.info("Reloaded local image tag to hash cache");
                ret = true;
            }
        }

        try (BufferedReader hdfsBr = getHdfsImageToHashReader()) {
            Map<String, String> hdfsImageToHash = readImageToHashFile(hdfsBr, hdfsImageToHashFile);
            if (hdfsImageToHash != null && !hdfsImageToHash.equals(hdfsImageToHashCache)) {
                hdfsImageToHashCache = hdfsImageToHash;
                LOG.info("Reloaded hdfs image tag to hash cache");
                ret = true;
            }
        }
        return ret;
    }

    private BufferedReader getLocalImageToHashReader() throws IOException {
        if (localImageTagToHashFile == null) {
            LOG.debug("Did not load local image to hash file, file is null");
            return null;
        }

        File imageTagToHashFile = new File(localImageTagToHashFile);
        if (!imageTagToHashFile.exists()) {
            LOG.warn("Did not load local image to hash file, file doesn't exist");
            return null;
        }

        long newLocalModTime = imageTagToHashFile.lastModified();
        if (newLocalModTime == localModTime) {
            LOG.debug("Did not load local image to hash file, file is unmodified");
            return null;
        }
        localModTime = newLocalModTime;

        return new BufferedReader(new FileReader(imageTagToHashFile));
    }

    private BufferedReader getHdfsImageToHashReader() throws IOException {
        if (hdfsImageToHashFile == null) {
            LOG.debug("Did not load hdfs image to hash file, file is null");
            return null;
        }

        Path imageToHash = new Path(hdfsImageToHashFile);
        FileSystem fs = imageToHash.getFileSystem(new Configuration());
        if (!fs.exists(imageToHash)) {
            String message = "Could not load hdfs image to hash file, " + hdfsImageToHashFile + " doesn't exist";
            LOG.error(message);
            throw new IOException(message);
        }

        long newHdfsModTime = fs.getFileStatus(imageToHash).getModificationTime();
        if (newHdfsModTime == hdfsModTime) {
            LOG.debug("Did not load hdfs image to hash file, file is unmodified");
            return null;
        }
        hdfsModTime = newHdfsModTime;

        return new BufferedReader(new InputStreamReader(fs.open(imageToHash)));
    }

    /**
     * Read the image-tag-to-hash file and parse as a Map.
     *
     * <p>You may specify multiple tags per hash all on the same line.
     * Comments are allowed using #. Anything after this character will not be read
     * Example file:
     *    foo/bar:current,fizz/gig:latest:123456789
     *    #this/line:wont,be:parsed:2378590895
     *
     * <p>This will map both foo/bar:current and fizz/gig:latest to 123456789
     */
    private static Map<String, String> readImageToHashFile(BufferedReader br, String filePath) throws IOException {
        if (br == null) {
            return null;
        }

        String line;
        Map<String, String> imageToHashCache = new HashMap<>();
        while ((line = br.readLine()) != null) {
            int index;
            index = line.indexOf("#");
            if (index == 0) {
                continue;
            } else if (index != -1) {
                line = line.substring(0, index);
            }

            index = line.lastIndexOf(":");
            if (index == -1) {
                LOG.warn("Malformed imageTagToManifest entry: {} in file: {}", line, filePath);
                continue;
            }
            String imageTags = line.substring(0, index);
            String[] imageTagArray = imageTags.split(",");
            String hash = line.substring(index + 1);

            if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
                LOG.warn("Malformed image hash: " + hash);
                continue;
            }

            for (String imageTag : imageTagArray) {
                imageToHashCache.put(imageTag, hash);
            }
        }
        return imageToHashCache;
    }


    @Override
    public synchronized ImageManifest getManifestFromImageTag(String imageTag) throws IOException {
        String hash = getHashFromImageTag(imageTag);
        ImageManifest manifest = manifestCache.get(hash);
        if (manifest != null) {
            return manifest;
        }
        Path manifestPath = new Path(manifestDir + hash);
        FileSystem fs = manifestPath.getFileSystem(new Configuration());
        FSDataInputStream input;
        try {
            input = fs.open(manifestPath);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Manifest file is not a valid HDFS file: "
                + manifestPath.toString(), iae);
        }

        byte[] bytes = IOUtils.toByteArray(input);
        manifest = objMapper.readValue(bytes, ImageManifest.class);

        manifestCache.put(hash, manifest);
        return manifest;
    }

    @Override
    public synchronized String getHashFromImageTag(String imageTag) {
        String hash;

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRefreshTime > Time.secsToMillis(ociCacheRefreshIntervalSecs)) {
            LOG.debug("Refreshing local and hdfs image-tag-to-hash cache");
            try {
                boolean loaded = loadImageToHashFiles();
                //If this is the first time trying to load the files and yet it failed
                if (!loaded && lastRefreshTime == 0) {
                    throw new RuntimeException("Couldn't load any image-tag-to-hash-files");
                }
                lastRefreshTime = currentTime;
            } catch (IOException e) {
                throw new RuntimeException("Couldn't load any image-tag-to-hash-files", e);
            }
        }

        // 1) Go to local file
        // 2) Go to HDFS
        // 3) Use tag as is/Assume tag is the hash
        if ((hash = localImageToHashCache.get(imageTag)) != null) {
            return hash;
        } else if ((hash = hdfsImageToHashCache.get(imageTag)) != null) {
            return hash;
        } else {
            return imageTag;
        }
    }

    private static class LruCache extends LinkedHashMap<String, ImageManifest> {
        private int cacheSize;

        LruCache(int initialCapacity, float loadFactor) {
            super(initialCapacity, loadFactor, true);
            this.cacheSize = initialCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, ImageManifest> eldest) {
            return this.size() > cacheSize;
        }
    }
}
