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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TreeNode {
    private String componentId;
    private int layer = -1;
    private List<TreeNode> parents = Lists.newArrayList();
    private List<TreeNode> children = Lists.newArrayList();
    private Set<String> sources = Sets.newHashSet();
    private Set<TreeNode> loopNodes = Sets.newHashSet();
    private int breadth = -1;

    public TreeNode(String componentId) {
        this.componentId = componentId;
    }

    public Set<TreeNode> getLoopNodes() {
        return loopNodes;
    }

    public void setLoopNodes(Set<TreeNode> loopNodes) {
        this.loopNodes = loopNodes;
    }

    public boolean addLoopNode(TreeNode node){
        if (loopNodes.contains(node) || node.getLoopNodes().contains(this)){
            return false;
        }else{
            loopNodes.add(node);
            node.getLoopNodes().add(this);
            return true;
        }
    }

    public boolean inCircle(TreeNode node){
        return sources.contains(node.componentId) &&
                node.getSources().contains(componentId);
    }

    public void addSource(String src){
        sources.add(src);
    }

    public boolean addSources(Set<String> srcs){
        int beforeSize = sources.size();
        sources.addAll(srcs);
        int afterSize = sources.size();
        return beforeSize == afterSize;
    }

    public Set<String> getSources() {
        return sources;
    }

    public boolean hasChildren(){
        return children.size() > 0;
    }

    public void addParent(TreeNode node){
        parents.add(node);
    }

    public void addChild(TreeNode node){
        children.add(node);
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public List<TreeNode> getParents() {
        return parents;
    }

    public void setParents(List<TreeNode> parents) {
        this.parents = parents;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

    public int getLayer() {
        return layer;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public boolean isVisited(){
        return layer != -1;
    }

    public int getBreadth() {
        return breadth;
    }

    public void setBreadth(int breadth) {
        this.breadth = breadth;
    }
}
