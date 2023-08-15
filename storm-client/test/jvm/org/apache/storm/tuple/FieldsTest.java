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

package org.apache.storm.tuple;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FieldsTest {

    @Test
    public void fieldsConstructorDoesNotThrowWithValidArgsTest() {
        assertEquals(new Fields("foo", "bar").size(), 2);
        assertEquals(new Fields("foo", "bar").size(), 2);
    }

    @Test
    public void duplicateFieldsNotAllowedWhenConstructingWithVarArgsTest() {
        assertThrows(IllegalArgumentException.class, () -> new Fields("foo", "bar", "foo"));
    }

    @Test
    public void duplicateFieldsNotAllowedTestWhenConstructingFromListTest() {
        assertThrows(IllegalArgumentException.class, () -> new Fields("foo", "bar", "foo"));
    }

    @Test
    public void getDoesNotThrowWithValidIndexTest() {
        Fields fields = new Fields("foo", "bar");
        assertEquals(fields.get(0), "foo");
        assertEquals(fields.get(1), "bar");
    }

    @Test
    public void getThrowsWhenOutOfBoundsTest() {
        assertThrows(IndexOutOfBoundsException.class, () -> {
            Fields fields = new Fields("foo", "bar");
            fields.get(2);
        });
    }

    @Test
    public void fieldIndexTest() {
        Fields fields = new Fields("foo", "bar");
        assertEquals(fields.fieldIndex("foo"), 0);
        assertEquals(fields.fieldIndex("bar"), 1);
    }

    @Test
    public void fieldIndexThrowsWhenOutOfBoundsTest() {
        assertThrows(IllegalArgumentException.class, () -> new Fields("foo").fieldIndex("baz"));
    }

    @Test
    public void containsTest() {
        Fields fields = new Fields("foo", "bar");
        assertTrue(fields.contains("foo"));
        assertTrue(fields.contains("bar"));
        assertFalse(fields.contains("baz"));
    }

    @Test
    public void toListTest() {
        Fields fields = new Fields("foo", "bar");
        List<String> fieldList = fields.toList();
        assertEquals(fieldList.size(), 2);
        assertEquals(fieldList.get(0), "foo");
        assertEquals(fieldList.get(1), "bar");
    }

    @Test
    public void toIteratorTest() {
        Fields fields = new Fields("foo", "bar");
        Iterator<String> fieldIter = fields.iterator();

        assertTrue(fieldIter.hasNext(), "First item is foo");
        assertEquals(fieldIter.next(), "foo");

        assertTrue(fieldIter.hasNext(), "Second item is bar");
        assertEquals(fieldIter.next(), "bar");

        assertFalse(fieldIter.hasNext(), "At end. hasNext should return false");
    }

    @Test
    public void selectTest() {
        Fields fields = new Fields("foo", "bar");
        List<Object> second = Arrays.asList(new Object[]{ "b" });
        List<Object> tuple = Arrays.asList(new Object[]{ "a", "b", "c" });
        List<Object> pickSecond = fields.select(new Fields("bar"), tuple);
        assertEquals(pickSecond, second);

        List<Object> secondAndFirst = Arrays.asList(new Object[]{ "b", "a" });
        List<Object> pickSecondAndFirst = fields.select(new Fields("bar", "foo"), tuple);
        assertEquals(pickSecondAndFirst, secondAndFirst);
    }

    @Test
    public void selectingUnknownFieldThrowsTest() {
        assertThrows(IllegalArgumentException.class, () -> {
            Fields fields = new Fields("foo", "bar");
            fields.select(new Fields("bar", "baz"), Arrays.asList(new Object[]{ "a", "b", "c" }));
        });
    }
}
