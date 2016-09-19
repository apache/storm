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

import org.jgrapht.DirectedGraph;

import java.util.ArrayList;
import java.util.List;

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


    public static boolean isSinkStream(String streamId) {
        return streamId.endsWith("__sink");
    }

    public static String getSinkStream(String streamId) {
        return streamId + "__sink";
    }

    public static boolean isPunctuation(Object value) {
        if (value instanceof Pair) {
            value = ((Pair) value).getFirst();
        }
        return WindowNode.PUNCTUATION.equals(value);
    }

}
