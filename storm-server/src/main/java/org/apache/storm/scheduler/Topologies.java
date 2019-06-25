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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class Topologies implements Iterable<TopologyDetails> {
    Map<String, TopologyDetails> topologies;
    Map<String, String> nameToId;
    Map<String, Map<String, Component>> allComponents;

    /**
     * Create a new Topologies from a list of TopologyDetails.
     *
     * @param details the list of details to use.
     * @throws IllegalArgumentException if duplicate topology ids are found.
     */
    public Topologies(TopologyDetails... details) {
        this(mkMap(details));
    }

    /**
     * Create a new Topologies from a map of id to topology.
     *
     * @param topologies a map of topology id to topology details
     */
    public Topologies(Map<String, TopologyDetails> topologies) {
        if (topologies == null) {
            topologies = Collections.emptyMap();
        }
        this.topologies = new HashMap<>(topologies);
        this.nameToId = new HashMap<>(topologies.size());

        for (Map.Entry<String, TopologyDetails> entry : topologies.entrySet()) {
            TopologyDetails topology = entry.getValue();
            this.nameToId.put(topology.getName(), entry.getKey());
        }
    }

    /**
     * Copy constructor.
     */
    public Topologies(Topologies src) {
        this(src.topologies);
    }

    private static Map<String, TopologyDetails> mkMap(TopologyDetails[] details) {
        Map<String, TopologyDetails> ret = new HashMap<>();
        for (TopologyDetails td : details) {
            if (ret.put(td.getId(), td) != null) {
                throw new IllegalArgumentException(
                    "Cannot have multiple topologies with the id " + td.getId());
            }
        }
        return ret;
    }

    public Collection<String> getAllIds() {
        return topologies.keySet();
    }

    /**
     * Get a topology given an ID.
     *
     * @param topologyId the id of the topology to get
     * @return the topology or null if it is not found
     */
    public TopologyDetails getById(String topologyId) {
        return topologies.get(topologyId);
    }

    /**
     * Get a topology given a topology name. Nimbus prevents multiple topologies from having the same name, so this assumes it is true.
     *
     * @param topologyName the name of the topology to look for
     * @return the a topology with the given name.
     */
    public TopologyDetails getByName(String topologyName) {
        String topologyId = this.nameToId.get(topologyName);

        if (topologyId == null) {
            return null;
        } else {
            return this.getById(topologyId);
        }
    }

    public Collection<TopologyDetails> getTopologies() {
        return topologies.values();
    }

    /**
     * Get all topologies submitted/owned by a given user.
     *
     * @param user the name of the user
     * @return all of the topologies submitted by this user.
     */
    public Collection<TopologyDetails> getTopologiesOwnedBy(String user) {
        HashSet<TopologyDetails> ret = new HashSet<>();
        for (TopologyDetails td : this) {
            if (user.equals(td.getTopologySubmitter())) {
                ret.add(td);
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append("Topologies:\n");
        for (TopologyDetails td : this.getTopologies()) {
            ret.append(td.toString()).append("\n");
        }
        return ret.toString();
    }

    @Override
    public Iterator<TopologyDetails> iterator() {
        return topologies.values().iterator();
    }
}
