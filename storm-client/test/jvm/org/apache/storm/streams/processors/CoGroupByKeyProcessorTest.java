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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.storm.streams.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CoGroupByKeyProcessorTest {
    private CoGroupByKeyProcessor<Integer, Integer, Integer> coGroupByKeyProcessor;
    private String firstStream = "first";
    private String secondStream = "second";
    private List<Pair<Integer, Pair<List<Integer>, List<Integer>>>> res = new ArrayList<>();

    private ProcessorContext context = new ProcessorContext() {
        @Override
        public <T> void forward(T input) {
            res.add((Pair<Integer, Pair<List<Integer>, List<Integer>>>) input);
        }

        @Override
        public <T> void forward(T input, String stream) {
        }

        @Override
        public boolean isWindowed() {
            return true;
        }

        @Override
        public Set<String> getWindowedParentStreams() {
            return null;
        }
    };

    private List<Pair<Integer, Integer>> firstKeyValues = Arrays.asList(
        Pair.of(2, 4),
        Pair.of(5, 25),
        Pair.of(7, 49),
        Pair.of(7, 87)
    );

    private List<Pair<Integer, Integer>> secondKeyValues = Arrays.asList(
        Pair.of(1, 1),
        Pair.of(2, 8),
        Pair.of(5, 125),
        Pair.of(5, 50),
        Pair.of(6, 216)

    );

    @Test
    public void testCoGroupByKey() throws Exception {
        coGroupByKeyProcessor = new CoGroupByKeyProcessor<>(firstStream, secondStream);
        processValues();
        List<Pair<Integer, Pair<Collection<Integer>, Collection<Integer>>>> expected = new ArrayList<>();
        Collection<Integer> list1 = new ArrayList<>();
        list1.add(25);
        Collection<Integer> list2 = new ArrayList<>();
        list2.add(125);
        list2.add(50);
        expected.add(Pair.of(5, Pair.of(list1, list2)));
        assertEquals(expected.get(0), res.get(1));
        list1.clear();
        list2.clear();
        list1.add(49);
        list1.add(87);
        expected.clear();
        expected.add(Pair.of(7, Pair.of(list1, list2)));
        assertEquals(expected.get(0), res.get(2));
    }


    private void processValues() {
        res.clear();
        coGroupByKeyProcessor.init(context);
        for (Pair<Integer, Integer> kv : firstKeyValues) {
            coGroupByKeyProcessor.execute(kv, firstStream);
        }
        for (Pair<Integer, Integer> kv : secondKeyValues) {
            coGroupByKeyProcessor.execute(kv, secondStream);
        }
        coGroupByKeyProcessor.finish();
    }

}
