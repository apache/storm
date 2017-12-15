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

package org.apache.storm.flux.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bean represenation of a topology.
 * <p/>
 * It consists of the following:
 *   1. The topology name
 *   2. A `java.util.Map` representing the `org.apache.storm.config` for the topology
 *   3. A list of spout definitions
 *   4. A list of bolt definitions
 *   5. A list of stream definitions that define the flow between spouts and bolts.
 *
 */
public class TopologyDef {
    private static Logger LOG = LoggerFactory.getLogger(TopologyDef.class);

    private String name;
    private Map<String, BeanDef> componentMap = new LinkedHashMap<String, BeanDef>(); // not required
    private List<IncludeDef> includes; // not required
    private Map<String, Object> config = new HashMap<String, Object>();

    // a "topology source" is a class that can produce a `StormTopology` thrift object.
    private TopologySourceDef topologySource;

    // the following are required if we're defining a core storm topology DAG in YAML, etc.
    private Map<String, BoltDef> boltMap = new LinkedHashMap<String, BoltDef>();
    private Map<String, SpoutDef> spoutMap = new LinkedHashMap<String, SpoutDef>();
    private List<StreamDef> streams = new ArrayList<StreamDef>();


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the name of the topology.
     * @param name topology name
     * @param override whether to override if already set
     */
    public void setName(String name, boolean override) {
        if (this.name == null || override) {
            this.name = name;
        } else {
            LOG.warn("Ignoring attempt to set property 'name' with override == false.");
        }
    }

    /**
     * Returns all spout definitions.
     * @return spout definitions.
     */
    public List<SpoutDef> getSpouts() {
        ArrayList<SpoutDef> retval = new ArrayList<SpoutDef>();
        retval.addAll(this.spoutMap.values());
        return retval;
    }

    /**
     * Set spout definitions.
     * @param spouts spout definitions
     */
    public void setSpouts(List<SpoutDef> spouts) {
        this.spoutMap = new LinkedHashMap<String, SpoutDef>();
        for (SpoutDef spout : spouts) {
            this.spoutMap.put(spout.getId(), spout);
        }
    }

    /**
     * Returns bolt definitions.
     * @return bolt definitions
     */
    public List<BoltDef> getBolts() {
        ArrayList<BoltDef> retval = new ArrayList<BoltDef>();
        retval.addAll(this.boltMap.values());
        return retval;
    }

    /**
     * Sets bolt definitions.
     * @param bolts bolt definitions
     */
    public void setBolts(List<BoltDef> bolts) {
        this.boltMap = new LinkedHashMap<>();
        for (BoltDef bolt : bolts) {
            this.boltMap.put(bolt.getId(), bolt);
        }
    }

    public List<StreamDef> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamDef> streams) {
        this.streams = streams;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * Returns a list of all component definitions.
     * @return components
     */
    public List<BeanDef> getComponents() {
        ArrayList<BeanDef> retval = new ArrayList<BeanDef>();
        retval.addAll(this.componentMap.values());
        return retval;
    }

    /**
     * Sets the list of component definitions.
     * @param components components definitions
     */
    public void setComponents(List<BeanDef> components) {
        this.componentMap = new LinkedHashMap<>();
        for (BeanDef component : components) {
            this.componentMap.put(component.getId(), component);
        }
    }

    public List<IncludeDef> getIncludes() {
        return includes;
    }

    public void setIncludes(List<IncludeDef> includes) {
        this.includes = includes;
    }

    // utility methods
    public int parallelismForBolt(String boltId) {
        return this.boltMap.get(boltId).getParallelism();
    }

    public BoltDef getBoltDef(String id) {
        return this.boltMap.get(id);
    }

    public SpoutDef getSpoutDef(String id) {
        return this.spoutMap.get(id);
    }

    public BeanDef getComponent(String id) {
        return this.componentMap.get(id);
    }

    /**
     * Adds a list of bolt definitions. Optionally overriding existing definitions
     * if one with the same ID already exists.
     * @param bolts bolt definitions
     * @param override whether or not to override existing definitions
     */
    public void addAllBolts(List<BoltDef> bolts, boolean override) {
        for (BoltDef bolt : bolts) {
            String id = bolt.getId();
            if (this.boltMap.get(id) == null || override) {
                this.boltMap.put(bolt.getId(), bolt);
            } else {
                LOG.warn("Ignoring attempt to create bolt '{}' with override == false.", id);
            }
        }
    }

    /**
     * Adds a list of spout definitions. Optionally overriding existing definitions
     * if one with the same ID already exists.
     * @param spouts spout definitions
     * @param override whether or not to override existing definitions
     */
    public void addAllSpouts(List<SpoutDef> spouts, boolean override) {
        for (SpoutDef spout : spouts) {
            String id = spout.getId();
            if (this.spoutMap.get(id) == null || override) {
                this.spoutMap.put(spout.getId(), spout);
            } else {
                LOG.warn("Ignoring attempt to create spout '{}' with override == false.", id);
            }
        }
    }

    /**
     * Adds a list of component definitions. Optionally overriding existing definitions
     * if one with the same ID already exists.
     * @param components component definitions
     * @param override whether or not to override existing definitions
     */
    public void addAllComponents(List<BeanDef> components, boolean override) {
        for (BeanDef bean : components) {
            String id = bean.getId();
            if (this.componentMap.get(id) == null || override) {
                this.componentMap.put(bean.getId(), bean);
            } else {
                LOG.warn("Ignoring attempt to create component '{}' with override == false.", id);
            }
        }
    }

    /**
     * Adds a list of stream definitions. Optionally overriding existing definitions
     * if one with the same ID already exists.
     * @param streams stream definitions
     * @param override whether or not to override existing definitions (currently ignored)
     */
    public void addAllStreams(List<StreamDef> streams, boolean override) {
        //TODO figure out how we want to deal with overrides. Users may want to add streams even when overriding other
        // properties. For now we just add them blindly which could lead to a potentially invalid topology.
        this.streams.addAll(streams);
    }

    public TopologySourceDef getTopologySource() {
        return topologySource;
    }

    public void setTopologySource(TopologySourceDef topologySource) {
        this.topologySource = topologySource;
    }

    public boolean isDslTopology() {
        return this.topologySource == null;
    }


    /**
     * Determines is this represents a valid Topology.
     * @return true if valid
     */
    public boolean validate() {
        boolean hasSpouts = this.spoutMap != null && this.spoutMap.size() > 0;
        boolean hasBolts = this.boltMap != null && this.boltMap.size() > 0;
        boolean hasStreams = this.streams != null && this.streams.size() > 0;
        // you cant define a topologySource and a DSL topology at the same time...
        if (!isDslTopology() && ((hasSpouts || hasBolts || hasStreams))) {
            return false;
        }
        if (isDslTopology() && (hasSpouts && hasBolts && hasStreams)) {
            return true;
        }
        return true;
    }
}
