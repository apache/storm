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

package org.apache.storm.streams.processors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.com.google.common.collect.ArrayListMultimap;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.ValueJoiner;
import org.apache.storm.streams.tuple.Tuple3;

/**
 * Provides equi-join implementation based on simple hash-join.
 */
public class JoinProcessor<K, R, V1, V2> extends BaseProcessor<Pair<K, ?>> implements BatchProcessor {
    private final ValueJoiner<V1, V2, R> valueJoiner;
    private final String leftStream;
    private final String rightStream;
    private final List<Pair<K, V1>> leftRows = new ArrayList<>();
    private final List<Pair<K, V2>> rightRows = new ArrayList<>();
    private final JoinType leftType;
    private final JoinType rightType;

    public JoinProcessor(String leftStream, String rightStream, ValueJoiner<V1, V2, R> valueJoiner) {
        this(leftStream, rightStream, valueJoiner, JoinType.INNER, JoinType.INNER);
    }

    public JoinProcessor(String leftStream, String rightStream, ValueJoiner<V1, V2, R> valueJoiner,
                         JoinType leftType, JoinType rightType) {
        this.valueJoiner = valueJoiner;
        this.leftStream = leftStream;
        this.rightStream = rightStream;
        this.leftType = leftType;
        this.rightType = rightType;
    }

    @Override
    public void execute(Pair<K, ?> input, String sourceStream) {
        K key = input.getFirst();
        if (sourceStream.equals(leftStream)) {
            V1 val = (V1) input.getSecond();
            Pair<K, V1> pair = Pair.of(key, val);
            leftRows.add(pair);
            if (!context.isWindowed()) {
                joinAndForward(Collections.singletonList(pair), rightRows);
            }
        } else if (sourceStream.equals(rightStream)) {
            V2 val = (V2) input.getSecond();
            Pair<K, V2> pair = Pair.of(key, val);
            rightRows.add(pair);
            if (!context.isWindowed()) {
                joinAndForward(leftRows, Collections.singletonList(pair));
            }
        }
    }

    @Override
    public void finish() {
        joinAndForward(leftRows, rightRows);
        leftRows.clear();
        rightRows.clear();
    }

    public String getLeftStream() {
        return leftStream;
    }

    public String getRightStream() {
        return rightStream;
    }

    /*
     * performs a hash-join by constructing a hash map of the smaller set, iterating over the
     * larger set and finding matching rows in the hash map.
     */
    private void joinAndForward(List<Pair<K, V1>> leftRows, List<Pair<K, V2>> rightRows) {
        if (leftRows.size() < rightRows.size()) {
            for (Tuple3<K, V1, V2> res : join(getJoinTable(leftRows), rightRows, leftType, rightType)) {
                context.forward(Pair.of(res.value1, valueJoiner.apply(res.value2, res.value3)));
            }
        } else {
            for (Tuple3<K, V2, V1> res : join(getJoinTable(rightRows), leftRows, rightType, leftType)) {
                context.forward(Pair.of(res.value1, valueJoiner.apply(res.value3, res.value2)));
            }
        }
    }

    /*
     * returns list of Tuple3 (key, val from table, val from row)
     */
    private <T1, T2> List<Tuple3<K, T1, T2>> join(Multimap<K, T1> tab, List<Pair<K, T2>> rows,
                                                  JoinType leftType, JoinType rightType) {
        List<Tuple3<K, T1, T2>> res = new ArrayList<>();
        for (Pair<K, T2> row : rows) {
            K key = row.getFirst();
            Collection<T1> values = tab.removeAll(key);
            if (values.isEmpty()) {
                if (rightType == JoinType.OUTER) {
                    res.add(new Tuple3<>(row.getFirst(), null, row.getSecond()));
                }
            } else {
                for (T1 mapValue : values) {
                    res.add(new Tuple3<>(row.getFirst(), mapValue, row.getSecond()));
                }
            }
        }
        // whatever remains in the tab are non matching left rows.
        if (leftType == JoinType.OUTER) {
            for (Map.Entry<K, T1> row : tab.entries()) {
                res.add(new Tuple3<>(row.getKey(), row.getValue(), null));
            }
        }
        return res;
    }

    /*
     * key1 -> (val1, val2 ..)
     * key2 -> (val3, val4 ..)
     */
    private <T> Multimap<K, T> getJoinTable(List<Pair<K, T>> rows) {
        Multimap<K, T> m = ArrayListMultimap.create();
        for (Pair<K, T> v : rows) {
            m.put(v.getFirst(), v.getSecond());
        }
        return m;
    }

    public enum JoinType {
        INNER,
        OUTER
    }
}
