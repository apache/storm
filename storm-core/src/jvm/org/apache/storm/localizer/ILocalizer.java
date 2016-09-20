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

import org.apache.storm.generated.LocalAssignment;

/**
 * Download blobs from the blob store and keep them up to date.
 */
public interface ILocalizer {

    /**
     * Recover a running topology by incrementing references for what it has already downloaded.
     * @param assignment the assignment the resources are for
     * @param port the port the topology is running in.
     */
    void recoverRunningTopology(LocalAssignment assignemnt, int port);
    
    /**
     * Download storm.jar, storm.conf, and storm.ser for this topology if not done so already,
     * and inc a reference count on them.
     * @param assignment the assignment the resources are for
     * @param port the port the topology is running on
     * @return a future to let you know when they are done.
     * @throws IOException on error 
     */
    Future<Void> requestDownloadBaseTopologyBlobs(LocalAssignment assignment, int port) throws IOException;

    /**
     * Download the blobs for this topology (reading in list in from the config)
     * and inc reference count for these blobs.
     * PRECONDITION: requestDownloadBaseTopologyBlobs has completed downloading.
     * @param assignment the assignment the resources are for
     * @param port the port the topology is running on
     * @return a future to let you know when they are done.
     */
    Future<Void> requestDownloadTopologyBlobs(LocalAssignment assignment, int port);
    
    /**
     * dec reference count on all blobs associated with this topology.
     * @param assignment the assignment the resources are for
     * @param port the port the topology is running on
     * @throws IOException on any error
     */
    void releaseSlotFor(LocalAssignment assignment, int port) throws IOException;
    
    /**
     * Clean up any topologies that are not in use right now.
     * @throws IOException on any error.
     */
    void cleanupUnusedTopologies() throws IOException;
}
