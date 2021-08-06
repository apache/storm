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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface OciResourcesLocalizerInterface {

    void init(Map<String, Object> conf) throws IOException;

    /**
     * Localize the oci resource.
     * @param ociResource the oci resource to be localized
     * @return the destination of the localized resource.
     * @throws IOException on I/O exception
     */
    String localize(OciResource ociResource) throws IOException;

    /**
     * Localize a list of oci resources.
     * @param resourceList a list of oci resources.
     * @return a list of destinations.
     * @throws IOException on I/O exception
     */
    default List<String> localize(List<OciResource> resourceList) throws IOException {
        List<String> resourceLocalDsts = new ArrayList<>();
        for (OciResource resource: resourceList) {
            resourceLocalDsts.add(localize(resource));
        }
        return resourceLocalDsts;
    }
}
