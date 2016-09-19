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
package org.apache.storm.streams;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Base class for a Node which form the vertices of the topology DAG.
 */
abstract class Node implements Serializable {
    private final Set<String> outputStreams;
    protected final Fields outputFields;
    protected String componentId;
    protected int parallelism;
    // the parent streams that this node subscribes to
    private final Multimap<Node, String> parentStreams = ArrayListMultimap.create();

    Node(Set<String> outputStreams, Fields outputFields, String componentId, int parallelism) {
        this.outputStreams = new HashSet<>(outputStreams);
        this.outputFields = outputFields;
        this.componentId = componentId;
        this.parallelism = parallelism;
    }

    Node(String outputStream, Fields outputFields, String componentId, int parallelism) {
        this(Collections.singleton(outputStream), outputFields, componentId, parallelism);
    }

    Node(String outputStream, Fields outputFields, String componentId) {
        this(outputStream, outputFields, componentId, 1);
    }

    Node(String outputStream, Fields outputFields) {
        this(outputStream, outputFields, null);
    }

    public Fields getOutputFields() {
        return outputFields;
    }

    String getComponentId() {
        return componentId;
    }

    void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    Integer getParallelism() {
        return parallelism;
    }

    void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    void addParentStream(Node parent, String streamId) {
        parentStreams.put(parent, streamId);
    }

    void removeParentStreams(Node parent) {
        parentStreams.removeAll(parent);
    }

    Set<String> getOutputStreams() {
        return Collections.unmodifiableSet(outputStreams);
    }

    Collection<String> getParentStreams(Node parent) {
        return parentStreams.get(parent);
    }

    Set<Node> getParents(String stream) {
        Multimap<String, Node> rev = Multimaps.invertFrom(parentStreams, ArrayListMultimap.<String, Node>create());
        return new HashSet<>(rev.get(stream));
    }

    void addOutputStream(String streamId) {
        outputStreams.add(streamId);
    }

    static Fields getOutputFields(IComponent component, String streamId) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        Map<String, StreamInfo> fieldsDeclaration = getter.getFieldsDeclaration();
        if ((fieldsDeclaration != null) && fieldsDeclaration.containsKey(streamId)) {
            return new Fields(fieldsDeclaration.get(streamId).get_output_fields());
        }
        return new Fields();
    }

    @Override
    public String toString() {
        return "Node{" +
                "outputStreams='" + outputStreams + '\'' +
                ", outputFields=" + outputFields +
                ", componentId='" + componentId + '\'' +
                ", parallelism=" + parallelism +
                '}';
    }
}
