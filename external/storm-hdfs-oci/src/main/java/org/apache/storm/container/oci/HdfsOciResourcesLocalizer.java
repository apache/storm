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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.HadoopLoginUtil;
import org.apache.storm.utils.ObjectReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsOciResourcesLocalizer implements OciResourcesLocalizerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsOciResourcesLocalizer.class);
    private static final int LOCALIZE_MAX_RETRY = 5;
    private String layersLocalDir;
    private String configLocalDir;
    private FileSystem fs;

    /**
     * Initialization.
     * @param conf the storm conf.
     * @throws IOException on I/O exception
     */
    public void init(Map<String, Object> conf) throws IOException {
        //login to hdfs
        HadoopLoginUtil.loginHadoop(conf);

        String resourcesLocalDir = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_RESOURCES_LOCAL_DIR),
            ConfigUtils.supervisorLocalDir(conf) + "/oci-resources");
        FileUtils.forceMkdir(new File(resourcesLocalDir));
        this.layersLocalDir = resourcesLocalDir + "/layers/";
        this.configLocalDir = resourcesLocalDir + "/config/";
        String topLevelDir = ObjectReader.getString(conf.get(DaemonConfig.STORM_OCI_IMAGE_HDFS_TOPLEVEL_DIR));
        this.fs = new Path(topLevelDir).getFileSystem(new Configuration());
    }

    /**
     * Download the resources from HDFS to local dir.
     * @param ociResource The oci resource to download
     * @return the destination of the oci resource
     * @throws IOException on I/O exception
     */
    public synchronized String localize(OciResource ociResource) throws IOException {
        if (ociResource == null) {
            return null;
        }
        File dst;
        switch (ociResource.getType()) {
            case CONFIG:
                dst = new File(this.configLocalDir, ociResource.getFileName());
                break;
            case LAYER:
                dst = new File(layersLocalDir, ociResource.getFileName());
                break;
            default:
                throw new IOException("unknown OciResourceType " + ociResource.getType());
        }

        if (dst.exists()) {
            LOG.info("{} already exists. Skip", dst);
        } else {
            // create working dir, copy file here, and set readable, then move to final location.
            // this allows the operation to be atomic in case the supervisor dies.
            File workingDir = new File(dst.getParent() + "/working");
            if (!workingDir.exists()) {
                boolean dirCreated = workingDir.mkdirs();
                if (!dirCreated) {
                    throw new IOException("Couldn't create the directory: " + workingDir);
                }
            }

            File workingDst = new File(workingDir.getPath() + "/" + dst.getName());

            LOG.info("Starting to copy {} from hdfs to {}", ociResource.getPath(), workingDst);
            copyFileLocallyWithRetry(ociResource, workingDst);
            LOG.info("Successfully finished copying {} from hdfs to {}", ociResource.getPath(), workingDst);

            //set to readable by anyone
            boolean setReadable = workingDst.setReadable(true, false);
            if (!setReadable) {
                throw new IOException("Couldn't set " + workingDst + " to be world-readable");
            }

            boolean fileRenamed = workingDst.renameTo(dst);
            if (!fileRenamed) {
                throw new IOException("Couldn't move " + workingDst + " to " + dst);
            }
        }
        return dst.toString();
    }

    private synchronized void copyFileLocallyWithRetry(OciResource ociResource, File dst) throws IOException {
        IOException lastIoException = null;

        for (int retryCount = 0; retryCount < LOCALIZE_MAX_RETRY; retryCount++) {
            try {
                fs.copyToLocalFile(new Path(ociResource.getPath()), new Path(dst.toString()));
                lastIoException = null;
                break;
            } catch (IOException e) {
                if (dst.exists()) {
                    FileDeleteStrategy.FORCE.delete(dst);
                }
                LOG.warn("{} occurred at attempt {}, deleted corrupt file {} if present", e.toString(), retryCount, dst);
                lastIoException = e;
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Failed to copy " + ociResource + " to " + dst, ie);
                }
            }
        }
        if (lastIoException != null) {
            LOG.error("Resource localization of {} to {} failed after {} retries", ociResource, dst, LOCALIZE_MAX_RETRY, lastIoException);
            throw lastIoException;
        }

    }
}
