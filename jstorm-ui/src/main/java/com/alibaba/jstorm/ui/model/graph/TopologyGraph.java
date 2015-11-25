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
package com.alibaba.jstorm.ui.model.graph;

import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TopologyGraph {
    List<TopologyNode> nodes;
    List<TopologyEdge> edges;
    int depth;
    int breadth;

    public TopologyGraph(List<TopologyNode> nodes, List<TopologyEdge> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    public List<TopologyNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<TopologyNode> nodes) {
        this.nodes = nodes;
    }

    public List<TopologyEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<TopologyEdge> edges) {
        this.edges = edges;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getBreadth() {
        return breadth;
    }

    public void setBreadth(int breadth) {
        this.breadth = breadth;
    }
}