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
import org.apache.storm.shade.com.google.common.collect.ArrayListMultimap;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.streams.Pair;

/**
 * co-group by key implementation.
 */
public class CoGroupByKeyProcessor<K, V1, V2> extends BaseProcessor<Pair<K, ?>> implements BatchProcessor {
    private final String firstStream;
    private final String secondStream;
    private final Multimap<K, V1> firstMap = ArrayListMultimap.create();
    private final Multimap<K, V2> secondMap = ArrayListMultimap.create();


    public CoGroupByKeyProcessor(String firstStream, String secondStream) {
        this.firstStream = firstStream;
        this.secondStream = secondStream;
    }

    @Override
    public void execute(Pair<K, ?> input, String sourceStream) {
        K key = input.getFirst();
        if (sourceStream.equals(firstStream)) {
            V1 val = (V1) input.getSecond();
            firstMap.put(key, val);
        } else if (sourceStream.equals(secondStream)) {
            V2 val = (V2) input.getSecond();
            secondMap.put(key, val);
        }
        if (!context.isWindowed()) {
            forwardValues();
        }

    }

    @Override
    public void finish() {
        forwardValues();
        firstMap.clear();
        secondMap.clear();
    }

    private void forwardValues() {
        firstMap.asMap().forEach((key, values) -> {
            context.forward(Pair.of(key, Pair.of(new ArrayList<>(values), secondMap.removeAll(key))));
        });

        secondMap.asMap().forEach((key, values) -> {
            context.forward(Pair.of(key, Pair.of(firstMap.removeAll(key), new ArrayList<>(values))));
        });

    }


}
