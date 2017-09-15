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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.storm.generated.LocalAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalDownloadedResource {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDownloadedResource.class);
    private static class PortNAssignment {
        private final int _port;
        private final LocalAssignment _assignment;
        
        public PortNAssignment(int port, LocalAssignment assignment) {
            _port = port;
            _assignment = assignment;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PortNAssignment)) {
                return false;
            }
            PortNAssignment pna = (PortNAssignment) other;
            return pna._port == _port && _assignment.equals(pna._assignment); 
        }
        
        @Override
        public int hashCode() {
            return (17 * _port) + _assignment.hashCode();
        }
        
        @Override
        public String toString() {
            return "{"+ _port + " " + _assignment +"}";
        }
    }
    private final CompletableFuture<Void> _pending;
    private final Set<PortNAssignment> _references;
    private boolean _isDone;
    
    
    public LocalDownloadedResource(CompletableFuture<Void> pending) {
        _pending = pending;
        _references = new HashSet<>();
        _isDone = false;
    }

    /**
     * Reserve the resources
     * @param port the port this is for
     * @param la the assignment this is for
     * @return a future that can be used to track it being downloaded.
     */
    public synchronized CompletableFuture<Void> reserve(int port, LocalAssignment la) {
        PortNAssignment pna = new PortNAssignment(port, la);
        if (!_references.add(pna)) {
            LOG.warn("Resources {} already reserved {} for this topology", pna, _references);
        }
        return _pending;
    }
    
    /**
     * Release a port from the reference count, and update isDone if all is done.
     * @param port the port to release
     * @param la the assignment to release
     * @return true if the port was being counted else false
     */
    public synchronized boolean release(int port, LocalAssignment la) {
        PortNAssignment pna = new PortNAssignment(port, la);
        boolean ret = _references.remove(pna);
        if (ret && _references.isEmpty()) {
            _isDone = true;
        }
        return ret;
    }
    
    /**
     * Is this has been cleaned up completely.
     * @return true if it is done else false
     */
    public synchronized boolean isDone() {
        return _isDone;
    }

    @Override
    public String toString() {
        return _references.toString();
    }
}
