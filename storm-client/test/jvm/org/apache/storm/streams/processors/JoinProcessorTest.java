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
import java.util.List;
import java.util.Set;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.PairValueJoiner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JoinProcessorTest {
    JoinProcessor<Integer, Pair<Integer, Integer>, Integer, Integer> joinProcessor;
    String leftStream = "left";
    String rightStream = "right";
    List<Pair<Integer, List<Pair<Integer, Integer>>>> res = new ArrayList<>();

    ProcessorContext context = new ProcessorContext() {
        @Override
        public <T> void forward(T input) {
            res.add((Pair<Integer, List<Pair<Integer, Integer>>>) input);
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

    List<Pair<Integer, Integer>> leftKeyValeus = Arrays.asList(
        Pair.of(2, 4),
        Pair.of(5, 25),
        Pair.of(7, 49)
    );

    List<Pair<Integer, Integer>> rightKeyValues = Arrays.asList(
        Pair.of(1, 1),
        Pair.of(2, 8),
        Pair.of(5, 125),
        Pair.of(6, 216)
    );

    @Test
    public void testInnerJoin() throws Exception {
        joinProcessor = new JoinProcessor<>(leftStream, rightStream, new PairValueJoiner<>());
        processValues();
        assertEquals(Pair.of(2, Pair.of(4, 8)), res.get(0));
        assertEquals(Pair.of(5, Pair.of(25, 125)), res.get(1));
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        joinProcessor = new JoinProcessor<>(leftStream, rightStream, new PairValueJoiner<>(),
                                            JoinProcessor.JoinType.OUTER, JoinProcessor.JoinType.INNER);
        processValues();
        assertEquals(Pair.of(2, Pair.of(4, 8)), res.get(0));
        assertEquals(Pair.of(5, Pair.of(25, 125)), res.get(1));
        assertEquals(Pair.of(7, Pair.of(49, null)), res.get(2));
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        joinProcessor = new JoinProcessor<>(leftStream, rightStream, new PairValueJoiner<>(),
                                            JoinProcessor.JoinType.INNER, JoinProcessor.JoinType.OUTER);
        processValues();
        assertEquals(Pair.of(1, Pair.of(null, 1)), res.get(0));
        assertEquals(Pair.of(2, Pair.of(4, 8)), res.get(1));
        assertEquals(Pair.of(5, Pair.of(25, 125)), res.get(2));
        assertEquals(Pair.of(6, Pair.of(null, 216)), res.get(3));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        joinProcessor = new JoinProcessor<>(leftStream, rightStream, new PairValueJoiner<>(),
                                            JoinProcessor.JoinType.OUTER, JoinProcessor.JoinType.OUTER);
        processValues();
        assertEquals(Pair.of(1, Pair.of(null, 1)), res.get(0));
        assertEquals(Pair.of(2, Pair.of(4, 8)), res.get(1));
        assertEquals(Pair.of(5, Pair.of(25, 125)), res.get(2));
        assertEquals(Pair.of(6, Pair.of(null, 216)), res.get(3));
        assertEquals(Pair.of(7, Pair.of(49, null)), res.get(4));
    }

    private void processValues() {
        res.clear();
        joinProcessor.init(context);
        for (Pair<Integer, Integer> kv : leftKeyValeus) {
            joinProcessor.execute(kv, leftStream);
        }
        for (Pair<Integer, Integer> kv : rightKeyValues) {
            joinProcessor.execute(kv, rightStream);
        }
        joinProcessor.finish();
    }

}
