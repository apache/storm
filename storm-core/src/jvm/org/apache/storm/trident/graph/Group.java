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
package org.apache.storm.trident.graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jgrapht.DirectedGraph;
import org.apache.storm.trident.planner.Node;
import org.apache.storm.trident.util.IndexedEdge;
import org.apache.storm.trident.util.TridentUtils;

public class Group {
    public final Set<Node> nodes = new HashSet<>();
    private final DirectedGraph<Node, IndexedEdge> graph;
    private final String id = UUID.randomUUID().toString();
    
    public Group(DirectedGraph graph, List<Node> nodes) {
        this.graph = graph;
        this.nodes.addAll(nodes);
    }
    
    public Group(DirectedGraph graph, Node n) {
        this(graph, Arrays.asList(n));
    }
    
    public Group(Group g1, Group g2) {
        this.graph = g1.graph;
        nodes.addAll(g1.nodes);
        nodes.addAll(g2.nodes);
    }
    
    public Set<Node> outgoingNodes() {
        Set<Node> ret = new HashSet<>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getChildren(graph, n));
        }
        return ret;
    }
    
    public Set<Node> incomingNodes() {
        Set<Node> ret = new HashSet<>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getParents(graph, n));
        }        
        return ret;        
    }

    /**
     * In case no resources are specified, returns empty map.
     * In case differing types of resources are specified, throw.
     * Otherwise, add all the resources for a group.
     */
    public Map<String, Number> getResources(Map<String, Number> defaults) {
        if(defaults == null) {
            defaults = new HashMap<>();
        }

        Map<String, Number> resources = null;
        for(Node n: nodes) {
            if(resources == null) {
                // After this, resources should contain all the kinds of resources
                // we can count for the group. If we see a kind of resource in another
                // node not in resources.keySet(), we'll throw.
                resources = new HashMap<>(defaults);
                resources.putAll(n.getResources());
            }
            else {
                Map<String, Number> node_res = new HashMap<>(defaults);
                node_res.putAll(n.getResources());
                
                if(!node_res.keySet().equals(resources.keySet())) {
                    StringBuilder ops = new StringBuilder();
                    
                    for(Node nod : nodes) {
                        Set<String> resource_keys = new HashSet<>(defaults.keySet());
                        resource_keys.addAll(nod.getResources().keySet());
                        ops.append("\t[ " + nod.shortString() + ", Resources Set: " + resource_keys + " ]\n");
                    }

                    if(node_res.keySet().containsAll(resources.keySet())) {
                        Set<String> diffset = new HashSet<>(node_res.keySet());
                        diffset.removeAll(resources.keySet());
                        throw new RuntimeException("Found an operation with resources set which are not set in other operations in the group:\n" +
                                                   "\t[ " + n.shortString() + " ]: " + diffset + "\n" +
                                                   "Either set these resources in all other operations in the group, add a default setting, or remove the setting from this operation.\n" +
                                                   "The group at fault:\n" +
                                                   ops);
                    }
                    else if(resources.keySet().containsAll(node_res.keySet())) {
                        Set<String> diffset = new HashSet<>(resources.keySet());
                        diffset.removeAll(node_res.keySet());
                        throw new RuntimeException("Found an operation with resources unset which are set in other operations in the group:\n" +
                                                   "\t[ " + n.shortString() + " ]: " + diffset + "\n" +
                                                   "Either set these resources in all other operations in the group, add a default setting, or remove the setting from all other operations.\n" +
                                                   "The group at fault:\n" +
                                                   ops);
                    }
                }

                for(Map.Entry<String, Number> kv : node_res.entrySet()) {
                    String key = kv.getKey();
                    Number val = kv.getValue();

                    Number newval = new Double(val.doubleValue() + resources.get(key).doubleValue());
                    resources.put(key, newval);
                }
            }
        }
        return resources;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return id.equals(((Group) o).id);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }    
}
