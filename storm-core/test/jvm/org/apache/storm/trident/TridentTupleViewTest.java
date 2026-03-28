/*
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

package org.apache.storm.trident;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.storm.Testing;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.tuple.TridentTupleView.FreshOutputFactory;
import org.apache.storm.trident.tuple.TridentTupleView.OperationOutputFactory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.trident.tuple.TridentTupleView.RootFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Ported from trident/tuple_test.clj.
 */
class TridentTupleViewTest {

    @Test
    void testFresh() {
        FreshOutputFactory freshFactory = new FreshOutputFactory(new Fields("a", "b", "c"));
        TridentTuple tt = freshFactory.create(Arrays.asList(3, 2, 1));

        assertEquals(Arrays.asList(3, 2, 1), tt.getValues());
        assertEquals(3, tt.getValueByField("a"));
        assertEquals(2, tt.getValueByField("b"));
        assertEquals(1, tt.getValueByField("c"));
    }

    @Test
    void testProjection() {
        FreshOutputFactory freshFactory = new FreshOutputFactory(new Fields("a", "b", "c", "d", "e"));
        ProjectionFactory projectFactory = new ProjectionFactory(freshFactory, new Fields("d", "a"));

        TridentTuple tt = freshFactory.create(Arrays.asList(3, 2, 1, 4, 5));
        TridentTuple tt2 = freshFactory.create(Arrays.asList(9, 8, 7, 6, 10));

        TridentTuple pt = projectFactory.create(tt);
        TridentTuple pt2 = projectFactory.create(tt2);

        assertEquals(Arrays.asList(4, 3), pt.getValues());
        assertEquals(Arrays.asList(6, 9), pt2.getValues());

        assertEquals(4, pt.getValueByField("d"));
        assertEquals(3, pt.getValueByField("a"));
        assertEquals(6, pt2.getValueByField("d"));
        assertEquals(9, pt2.getValueByField("a"));
    }

    @Test
    void testAppends() {
        FreshOutputFactory freshFactory = new FreshOutputFactory(new Fields("a", "b", "c"));
        OperationOutputFactory appendFactory = new OperationOutputFactory(freshFactory, new Fields("d", "e"));
        OperationOutputFactory appendFactory2 = new OperationOutputFactory(appendFactory, new Fields("f"));

        TridentTupleView tt = (TridentTupleView) freshFactory.create(Arrays.asList(1, 2, 3));
        TridentTupleView tt2 = (TridentTupleView) appendFactory.create(tt, Arrays.asList(4, 5));
        TridentTuple tt3 = appendFactory2.create(tt2, Arrays.asList(7));

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 7), tt3.getValues());
        assertEquals(5, tt2.getValueByField("e"));
        assertEquals(5, tt3.getValueByField("e"));
        assertEquals(7, tt3.getValueByField("f"));
    }

    @Test
    void testRoot() {
        RootFactory rootFactory = new RootFactory(new Fields("a", "b"));
        Tuple stormTuple = Testing.testTuple(Arrays.asList("a", 1));
        TridentTupleView tt = (TridentTupleView) rootFactory.create(stormTuple);

        assertEquals(Arrays.asList("a", 1), tt.getValues());
        assertEquals("a", tt.getValueByField("a"));
        assertEquals(1, tt.getValueByField("b"));

        OperationOutputFactory appendFactory = new OperationOutputFactory(rootFactory, new Fields("c"));
        TridentTuple tt2 = appendFactory.create(tt, Arrays.asList(3));

        assertEquals(Arrays.asList("a", 1, 3), tt2.getValues());
        assertEquals("a", tt2.getValueByField("a"));
        assertEquals(1, tt2.getValueByField("b"));
        assertEquals(3, tt2.getValueByField("c"));
    }

    @Test
    void testComplex() {
        FreshOutputFactory freshFactory = new FreshOutputFactory(new Fields("a", "b", "c"));
        OperationOutputFactory appendFactory1 = new OperationOutputFactory(freshFactory, new Fields("d"));
        OperationOutputFactory appendFactory2 = new OperationOutputFactory(appendFactory1, new Fields("e", "f"));
        ProjectionFactory projectFactory1 = new ProjectionFactory(appendFactory2, new Fields("a", "f", "b"));
        OperationOutputFactory appendFactory3 = new OperationOutputFactory(projectFactory1, new Fields("c"));

        TridentTupleView tt = (TridentTupleView) freshFactory.create(Arrays.asList(1, 2, 3));
        TridentTupleView tt2 = (TridentTupleView) appendFactory1.create(tt, Arrays.asList(4));
        TridentTupleView tt3 = (TridentTupleView) appendFactory2.create(tt2, Arrays.asList(5, 6));
        TridentTupleView tt4 = (TridentTupleView) projectFactory1.create(tt3);
        TridentTuple tt5 = appendFactory3.create(tt4, Arrays.asList(8));

        assertEquals(Arrays.asList(1, 2, 3), tt.getValues());
        assertEquals(Arrays.asList(1, 2, 3, 4), tt2.getValues());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), tt3.getValues());
        assertEquals(Arrays.asList(1, 6, 2), tt4.getValues());
        assertEquals(Arrays.asList(1, 6, 2, 8), tt5.getValues());

        assertEquals(1, tt5.getValueByField("a"));
        assertEquals(6, tt5.getValueByField("f"));
        assertEquals(2, tt5.getValueByField("b"));
        assertEquals(8, tt5.getValueByField("c"));
    }

    @Test
    void testITupleInterface() {
        TridentTuple tt = TridentTupleView.createFreshTuple(new Fields("a", "b", "c"), Arrays.asList(1, 2, 3));

        assertEquals(Arrays.asList(1, 2, 3), tt.getValues());
        assertEquals(Arrays.asList("a", "b", "c"), tt.getFields().toList());
        assertTrue(tt.contains("a"));
        assertFalse(tt.contains("abcd"));
        assertEquals(0, tt.fieldIndex("a"));
        assertEquals(Arrays.asList(3, 1), tt.select(new Fields("c", "a")));
    }
}
