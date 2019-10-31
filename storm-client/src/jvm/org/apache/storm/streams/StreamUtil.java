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

package org.apache.storm.streams;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.tuple.Fields;

public class StreamUtil {
    @SuppressWarnings("unchecked")
    public static <T> List<T> getParents(DirectedGraph<Node, Edge> graph, Node node) {
        List<Edge> incoming = new ArrayList<>(graph.incomingEdgesOf(node));
        List<T> ret = new ArrayList<>();
        for (Edge e : incoming) {
            ret.add((T) e.getSource());
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> getChildren(DirectedGraph<Node, Edge> graph, Node node) {
        List<Edge> outgoing = new ArrayList<>(graph.outgoingEdgesOf(node));
        List<T> ret = new ArrayList<>();
        for (Edge e : outgoing) {
            ret.add((T) e.getTarget());
        }
        return ret;
    }

    public static boolean isPunctuation(Object value) {
        return PUNCTUATION.equals(value);
    }

    public static String getPunctuationStream(String stream) {
        return stream + PUNCTUATION;
    }

    public static String getSourceStream(String stream) {
        int idx = stream.lastIndexOf(PUNCTUATION);
        if (idx > 0) {
            return stream.substring(0, idx);
        }
        return stream;
    }

    public static Fields getPunctuationFields() {
        return new Fields(PUNCTUATION);
    }

}
