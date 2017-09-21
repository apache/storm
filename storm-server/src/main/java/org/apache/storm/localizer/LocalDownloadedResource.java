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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.storm.generated.LocalAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for accounting to keep track of who is waiting for specific resources to be downloaded.
 */
public class LocalDownloadedResource {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDownloadedResource.class);
    private final CompletableFuture<Void> pending;
    private final Set<PortAndAssignment> references;
    private boolean isDone;
    
    
    public LocalDownloadedResource(CompletableFuture<Void> pending) {
        this.pending = pending;
        references = new HashSet<>();
        isDone = false;
    }

    /**
     * Reserve the resources
     * @param port the port this is for
     * @param la the assignment this is for
     * @return a future that can be used to track it being downloaded.
     */
    public synchronized CompletableFuture<Void> reserve(int port, LocalAssignment la) {
        PortAndAssignment pna = new PortAndAssignment(port, la);
        if (!references.add(pna)) {
            LOG.warn("Resources {} already reserved {} for this topology", pna, references);
        }
        return pending;
    }
    
    /**
     * Release a port from the reference count, and update isDone if all is done.
     * @param port the port to release
     * @param la the assignment to release
     * @return true if the port was being counted else false
     */
    public synchronized boolean release(int port, LocalAssignment la) {
        PortAndAssignment pna = new PortAndAssignment(port, la);
        boolean ret = references.remove(pna);
        if (ret && references.isEmpty()) {
            isDone = true;
        }
        return ret;
    }
    
    /**
     * Is this has been cleaned up completely.
     * @return true if it is done else false
     */
    public synchronized boolean isDone() {
        return isDone;
    }

    @Override
    public String toString() {
        return references.toString();
    }
}
