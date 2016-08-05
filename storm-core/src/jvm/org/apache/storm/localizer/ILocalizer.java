/**
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
package org.apache.storm.localizer;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Download blobs from the blob store and keep them up to date.
 */
public interface ILocalizer {

    /**
     * Recover a running topology by incrementing references for what it has already downloaded.
     * @param topologyId the topology the slot is using
     * @param port the port the topology is running in.
     */
    void recoverRunningTopology(String topologyId, int port);
    
    /**
     * Download storm.jar, storm.conf, and storm.ser for this topology if not done so already,
     * and inc a reference count on them.
     * @param topologyId the id of the topology to download them for
     * @param port the port the topology is running on
     * @return a future to let you know when they are done.
     */
    Future<Void> requestDownloadBaseTopologyBlobs(String topologyId, int port);

    /**
     * Download the blobs for this topology (reading in list in from the config)
     * and inc reference count for these blobs.
     * PRECONDITION: requestDownloadBaseTopologyBlobs has completed downloading.
     * @param topologyId the id of the topology to download them for
     * @param port the port the topology is running on
     * @return a future to let you know when they are done.
     */
    Future<Void> requestDownloadTopologyBlobs(String topologyId, int port);
    
    /**
     * dec reference count on all blobs associated with this topology.
     * @param topologyId the topology to release
     * @param port the port the topology is running on
     * @throws IOException on any error
     */
    void releaseSlotFor(String topologyId, int port) throws IOException;
}
