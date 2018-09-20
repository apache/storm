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

package org.apache.storm.scheduler;

/**
 * A Cluster that only allows modification to a single topology.
 */
public class SingleTopologyCluster extends Cluster {
    private final String allowedId;

    /**
     * Create a new cluster that only allows modifications to a single topology.
     *
     * @param other      the current cluster to base this off of
     * @param topologyId the topology that is allowed to be modified.
     */
    public SingleTopologyCluster(Cluster other, String topologyId) {
        super(other);
        allowedId = topologyId;
    }

    @Override
    protected void assertValidTopologyForModification(String topologyId) {
        //AllowedId is null in the constructor, so it can assign what it needs/etc.
        if (allowedId != null && !allowedId.equals(topologyId)) {
            throw new IllegalArgumentException(
                "Only " + allowedId + " is allowed to be modified at this time.");
        }
    }
}
