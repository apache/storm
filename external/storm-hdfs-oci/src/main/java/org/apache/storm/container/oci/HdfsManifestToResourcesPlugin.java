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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.HadoopLoginUtil;
import org.apache.storm.utils.ObjectReader;

public class HdfsManifestToResourcesPlugin implements OciManifestToResourcesPluginInterface {

    private String layersDir;
    private String configDir;
    private FileSystem fs;
    private LoadingCache<Path, FileStatus> statCache;

    private static final String CONFIG_MEDIA_TYPE = "application/vnd.docker.container.image.v1+json";

    private static final String LAYER_TAR_GZIP_MEDIA_TYPE = "application/vnd.docker.image.rootfs.diff.tar.gzip";

    private static final String SHA_256 = "sha256";

    private static final String CONFIG_HASH_ALGORITHM = SHA_256;

    private static final String LAYER_HASH_ALGORITHM = SHA_256;

    private static final int SHA256_HASH_LENGTH = 64;

    private static final String ALPHA_NUMERIC = "[a-zA-Z0-9]+";

    @Override
    public void init(Map<String, Object> conf) throws IOException {

        //login to hdfs
        HadoopLoginUtil.loginHadoop(conf);

        String topLevelDir = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_IMAGE_HDFS_TOPLEVEL_DIR));

        this.layersDir = topLevelDir + "/layers/";
        this.configDir = topLevelDir + "/config/";

        this.fs = new Path(topLevelDir).getFileSystem(new Configuration());

        CacheLoader<Path, FileStatus> cacheLoader =
            new CacheLoader<Path, FileStatus>() {
                @Override
                public FileStatus load(@Nonnull Path path) throws Exception {
                    return statBlob(path);
                }
            };
        this.statCache = CacheBuilder.newBuilder().maximumSize(30)
            .refreshAfterWrite(60, TimeUnit.MINUTES).build(cacheLoader);
    }

    @Override
    public List<OciResource> getLayerResources(ImageManifest manifest) throws IOException {
        List<OciResource> ociResources = new ArrayList<>();
        for (ImageManifest.Blob blob : manifest.getLayers()) {
            String mediaType = blob.getMediaType();
            if (!mediaType.equals(LAYER_TAR_GZIP_MEDIA_TYPE)) {
                throw new IOException("Invalid layer mediaType: " + mediaType);
            }

            String[] layerDigest = blob.getDigest().split(":", 2);
            String algorithm = layerDigest[0];
            if (!algorithm.equals(LAYER_HASH_ALGORITHM)) {
                throw new IOException("Invalid layer digest algorithm: " + algorithm);
            }

            String hash = layerDigest[1];
            if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
                throw new IOException("Malformed layer digest: " + hash);
            }

            long size = blob.getSize();
            String fileName = hash + ".sqsh";
            Path path = new Path(layersDir, fileName);

            try {
                FileStatus stat = statCache.get(path);
                long timestamp = stat.getModificationTime();

                OciResource ociResource = new OciResource(path.toString(), fileName, size, timestamp, OciResource.OciResourceType.LAYER);
                ociResources.add(ociResource);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }
        return ociResources;
    }

    @Override
    public OciResource getConfigResource(ImageManifest manifest) throws IOException {
        ImageManifest.Blob config = manifest.getConfig();

        String mediaType = config.getMediaType();
        if (!mediaType.equals(CONFIG_MEDIA_TYPE)) {
            throw new IOException("Invalid config mediaType: " + mediaType);
        }

        String[] configDigest = config.getDigest().split(":", 2);

        String algorithm = configDigest[0];
        if (!algorithm.equals(CONFIG_HASH_ALGORITHM)) {
            throw new IOException("Invalid config digest algorithm: " + algorithm);
        }

        String hash = configDigest[1];
        if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
            throw new IOException("Malformed config digest: " + hash);
        }

        long size = config.getSize();
        Path path = new Path(configDir + hash);

        OciResource ociResource;

        try {
            FileStatus stat = statCache.get(path);
            long timestamp = stat.getModificationTime();
            ociResource = new OciResource(path.toString(), hash, size, timestamp, OciResource.OciResourceType.CONFIG);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }

        return ociResource;
    }

    private FileStatus statBlob(Path path) throws IOException {
        return fs.getFileStatus(path);
    }
}
