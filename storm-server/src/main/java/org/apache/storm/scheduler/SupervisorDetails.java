/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorDetails {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorDetails.class);

    private final String id;
    /**
     * thrift server of this supervisor.
     */
    private final Integer serverPort;
    /**
     * hostname of this supervisor.
     */
    private final String host;
    private final Object meta;
    /**
     * meta data configured for this supervisor.
     */
    private final Object schedulerMeta;
    /**
     * Map containing a manifest of resources for the node the supervisor resides.
     */
    private final NormalizedResourceOffer totalResources;
    /**
     * all the ports of the supervisor.
     */
    private Set<Integer> allPorts;

    /**
     * Create the details of a new supervisor.
     * @param id the ID as reported by the supervisor.
     * @param serverPort the thrift server for the supervisor.
     * @param host the host the supervisor is on.
     * @param meta meta data reported by the supervisor (should be a collection of the ports on the supervisor).
     * @param schedulerMeta Not used and can probably be removed.
     * @param allPorts all of the ports for the supervisor (a better version of meta)
     * @param totalResources all of the resources for this supervisor.
     */
    public SupervisorDetails(String id, Integer serverPort, String host, Object meta, Object schedulerMeta,
                             Collection<? extends Number> allPorts, Map<String, Double> totalResources) {

        this.id = id;
        this.serverPort = serverPort;
        this.host = host;
        this.meta = meta;
        this.schedulerMeta = schedulerMeta;
        if (allPorts != null) {
            setAllPorts(allPorts);
        } else {
            this.allPorts = new HashSet<>();
        }
        this.totalResources = new NormalizedResourceOffer(totalResources);
        LOG.debug("Creating a new supervisor ({}-{}) with resources: {}", this.host, this.id, totalResources);
    }

    public SupervisorDetails(String id, Object meta) {
        this(id, null, null, meta, null, null, null);
    }

    public SupervisorDetails(String id, Object meta, Map<String, Double> totalResources) {
        this(id, null, null, meta, null, null, totalResources);
    }

    public SupervisorDetails(String id, Object meta, Collection<? extends Number> allPorts) {
        this(id, null, null, meta, null, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<? extends Number> allPorts) {
        this(id, null, host, null, schedulerMeta, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta,
                             Collection<? extends Number> allPorts, Map<String, Double> totalResources) {
        this(id, null, host, null, schedulerMeta, allPorts, totalResources);
    }

    public SupervisorDetails(String id, int serverPort, String host, Object schedulerMeta,
                             Collection<? extends Number> allPorts, Map<String, Double> totalResources) {
        this(id, serverPort, host, null, schedulerMeta, allPorts, totalResources);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " ID: " + id + " HOST: " + host + " META: " + meta
               + " SCHED_META: " + schedulerMeta + " PORTS: " + allPorts;
    }

    public String getId() {
        return id;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getHost() {
        return host;
    }

    public Object getMeta() {
        return meta;
    }

    public Set<Integer> getAllPorts() {
        return allPorts;
    }

    private void setAllPorts(Collection<? extends Number> allPorts) {
        this.allPorts = new HashSet<>();
        if (allPorts != null) {
            for (Number n : allPorts) {
                this.allPorts.add(n.intValue());
            }
        }
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }

    /**
     * Get the total Memory on this supervisor in MB.
     */
    public double getTotalMemory() {
        return totalResources.getTotalMemoryMb();
    }

    /**
     * Get the total CPU on this supervisor in % CPU.
     */
    public double getTotalCpu() {
        return totalResources.getTotalCpu();
    }

    /*
     * Get the total Generic resources on this supervisor.
     */
    public Map<String, Double> getTotalGenericResources() {
        Map<String, Double> genericResources = totalResources.toNormalizedMap();
        NormalizedResourceRequest.removeNonGenericResources(genericResources);
        return genericResources;
    }

    /**
     * Get all resources for this Supervisor.
     */
    public NormalizedResourceOffer getTotalResources() {
        return totalResources;
    }
}
