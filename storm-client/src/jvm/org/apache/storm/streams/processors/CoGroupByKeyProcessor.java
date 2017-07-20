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

package org.apache.storm.streams.processors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.PairValueJoiner;
import org.apache.storm.streams.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * co-group by key implementation
 */
public class CoGroupByKeyProcessor<K, V1, V2> extends BaseProcessor<Pair<K, ?>> implements BatchProcessor {
    private final PairValueJoiner<Collection<V1>, Collection<V2>> valueJoiner;
    private final String firstStream;
    private final String secondStream;
    private final List<Pair<K, V1>> firstRows = new ArrayList<>();
    private final List<Pair<K, V2>> secondRows = new ArrayList<>();

    public CoGroupByKeyProcessor(String firstStream, String secondStream, PairValueJoiner<Collection<V1>, Collection<V2>> valueJoiner) {
        this.valueJoiner = valueJoiner;
        this.firstStream = firstStream;
        this.secondStream = secondStream;
    }

    @Override
    public void execute(Pair<K, ?> input, String sourceStream) {
        K key = input.getFirst();
        if (sourceStream.equals(firstStream)) {
            V1 val = (V1) input.getSecond();
            Pair<K, V1> pair = Pair.of(key, val);
            firstRows.add(pair);
        } else if (sourceStream.equals(secondStream)) {
            V2 val = (V2) input.getSecond();
            Pair<K, V2> pair = Pair.of(key, val);
            secondRows.add(pair);
        }
        if (!context.isWindowed()) {
            joinAndForward(firstRows, secondRows);
        }

    }

    @Override
    public void finish() {
        joinAndForward(firstRows, secondRows);
        firstRows.clear();
        secondRows.clear();
    }

    private void joinAndForward(List<Pair<K, V1>> firstRows, List<Pair<K, V2>> secondRows) {
        for (Tuple3<K, Collection<V1>, Collection<V2>> res : join(getJoinTable(firstRows), getJoinTable(secondRows))) {
                context.forward(Pair.of(res._1, valueJoiner.apply(res._2, res._3)));

        }
    }

    /*
     * returns list of Tuple3 (key, val from table, val from row)
     */

    private <T1, T2> List<Tuple3<K, Collection<T1>, Collection<T2>>> join(Multimap<K, T1> tab1, Multimap<K, T2> tab2) {
        List<Tuple3<K, Collection<T1> ,Collection<T2> >> res = new ArrayList<>();
        for (K key : tab1.keys()) {
            Collection<T2> values = tab2.removeAll(key);
            res.add(new Tuple3<>(key, tab1.get(key), values));
        }
        // whatever remains in the tab2 are non matching tab1.
        for (K key2 : tab2.keys()) {
            res.add(new Tuple3<>(key2, new ArrayList<T1>(), tab2.get(key2)));
        }

        return res;
    }

    private <T> Multimap<K, T> getJoinTable(List<Pair<K, T>> rows) {
        Multimap<K, T> m = ArrayListMultimap.create();
        for (Pair<K, T> v : rows) {
            m.put(v.getFirst(), v.getSecond());
        }
        return m;
    }

}
