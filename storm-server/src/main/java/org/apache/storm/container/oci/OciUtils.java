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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.WrappedInvalidTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OciUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OciUtils.class);

    /**
     * Adjust the image config for the topology.
     * If OCI container is not supported, remove the oci image setting from the topoConf;
     * otherwise, set it to the default image if it's null.
     * @param conf the daemon conf
     * @param topoConf the topology conf
     * @param topoId the topology Id
     * @throws InvalidTopologyException if image config is invalid
     */
    public static void adjustImageConfigForTopo(Map<String, Object> conf, Map<String, Object> topoConf, String topoId)
        throws InvalidTopologyException {

        //don't need sanity check here as we assume it's already done during daemon startup
        List<String> allowedImages = getAllowedImages(conf, false);
        String topoImage = (String) topoConf.get(Config.TOPOLOGY_OCI_IMAGE);

        if (allowedImages.isEmpty()) {
            if (topoImage != null) {
                LOG.warn("{} is not configured; this indicates OCI container is not supported; "
                        + "{} config for topology {} will be removed",
                    DaemonConfig.STORM_OCI_ALLOWED_IMAGES, Config.TOPOLOGY_OCI_IMAGE, topoId);
                topoConf.remove(Config.TOPOLOGY_OCI_IMAGE);
            }
        } else {
            if (topoImage == null) {
                //we assume the default image is already validated during daemon startup
                String defaultImage = (String) conf.get(DaemonConfig.STORM_OCI_IMAGE);
                topoImage = defaultImage;
                topoConf.put(Config.TOPOLOGY_OCI_IMAGE, topoImage);
                LOG.info("{} is not set for topology {}; set it to the default image {} configured in {}",
                    Config.TOPOLOGY_OCI_IMAGE, topoId, defaultImage, DaemonConfig.STORM_OCI_IMAGE);
            } else {
                try {
                    validateImage(allowedImages, topoImage, Config.TOPOLOGY_OCI_IMAGE);
                } catch (IllegalArgumentException e) {
                    throw new WrappedInvalidTopologyException(e.getMessage());
                }
            }
        }
    }

    /**
     * Validates the image setting in the daemon conf.
     * This will be skipped if {@link DaemonConfig#STORM_OCI_ALLOWED_IMAGES} not configured.
     * @param conf the daemon conf
     */
    public static void validateImageInDaemonConf(Map<String, Object> conf) {
        List<String> allowedImages = getAllowedImages(conf, true);
        if (allowedImages.isEmpty()) {
            LOG.debug("{} is not configured; skip image validation", DaemonConfig.STORM_OCI_ALLOWED_IMAGES);
        } else {
            String defaultImage = (String) conf.get(DaemonConfig.STORM_OCI_IMAGE);
            validateImage(allowedImages, defaultImage, DaemonConfig.STORM_OCI_IMAGE);
        }
    }

    private static final String OCI_IMAGE_PATTERN = "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
    private static final Pattern ociImagePattern = Pattern.compile(OCI_IMAGE_PATTERN);
    /**
     * special case for allowing all images; should only be used in {@link DaemonConfig#STORM_OCI_ALLOWED_IMAGES}.
     */
    private static final String ASTERISK = "*";

    /**
     * This is a helper function to validate the image.
     * @param allowedImages the allowed image list
     * @param imageToValidate the image to be validated
     * @param imageConfigKey the config where this image comes from; this is for logging purpose.
     */
    private static void validateImage(List<String> allowedImages, String imageToValidate, String imageConfigKey) {
        if (imageToValidate == null) {
            throw new IllegalArgumentException(imageConfigKey + " is null");
        }

        if (!allowedImages.contains(ASTERISK) && !allowedImages.contains(imageToValidate)) {
            throw new IllegalArgumentException(imageConfigKey + "=" + imageToValidate
                + " is not in the list of " + DaemonConfig.STORM_OCI_ALLOWED_IMAGES + ": "
                + allowedImages);
        }

        if (!ociImagePattern.matcher(imageToValidate).matches()) {
            throw new IllegalArgumentException(imageConfigKey + "=" + imageToValidate
                + " doesn't match the pattern " + OCI_IMAGE_PATTERN);
        }
    }

    private static List<String> getAllowedImages(Map<String, Object> conf, boolean validationEnforced) {
        List<String> allowedImages = ObjectReader.getStrings(conf.get(DaemonConfig.STORM_OCI_ALLOWED_IMAGES));

        if (validationEnforced) {
            //check if image name matches the required pattern
            for (String image : allowedImages) {
                if (!image.equals(ASTERISK) && !ociImagePattern.matcher(image).matches()) {
                    throw new IllegalArgumentException(image + " in the list of "
                        + DaemonConfig.STORM_OCI_ALLOWED_IMAGES
                        + " doesn't match the pattern " + OCI_IMAGE_PATTERN
                        + " or is not " + ASTERISK);
                }
            }
        }
        return allowedImages;
    }
}
