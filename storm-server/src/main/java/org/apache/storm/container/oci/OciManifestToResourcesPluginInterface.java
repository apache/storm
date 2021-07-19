/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.storm.container.oci;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface OciManifestToResourcesPluginInterface {

    /**
     * Initialization.
     * @param conf the storm conf
     * @throws IOException on I/O exception
     */
    void init(Map<String, Object> conf) throws IOException;

    /**
     * Get the layers information from the manifest.
     * The layers should be returned in the order in which they appear in the manifest
     * @param manifest the manifest of a image
     * @return a list of layers information
     * @throws IOException on I/O exception
     */
    List<OciResource> getLayerResources(ImageManifest manifest) throws IOException;

    /**
     * Get the image config information from the manifest.
     * @param manifest the manifest of a image
     * @return the config of this image
     * @throws IOException on I/O exception
     */
    OciResource getConfigResource(ImageManifest manifest) throws IOException;
}
