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
import org.junit.Assert;
import org.junit.Test;

public class FieldsTest {

    @Test
    public void fieldsConstructorDoesNotThrowWithValidArgsTest() {
        Assert.assertEquals(new Fields("foo", "bar").size(), 2);
        Assert.assertEquals(new Fields(new String[]{ "foo", "bar" }).size(), 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateFieldsNotAllowedWhenConstructingWithVarArgsTest() {
        new Fields("foo", "bar", "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateFieldsNotAllowedTestWhenConstructingFromListTest() {
        new Fields(new String[]{ "foo", "bar", "foo" });
    }

    @Test
    public void getDoesNotThrowWithValidIndexTest() {
        Fields fields = new Fields("foo", "bar");
        Assert.assertEquals(fields.get(0), "foo");
        Assert.assertEquals(fields.get(1), "bar");
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getThrowsWhenOutOfBoundsTest() {
        Fields fields = new Fields("foo", "bar");
        fields.get(2);
    }

    @Test
    public void fieldIndexTest() {
        Fields fields = new Fields("foo", "bar");
        Assert.assertEquals(fields.fieldIndex("foo"), 0);
        Assert.assertEquals(fields.fieldIndex("bar"), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fieldIndexThrowsWhenOutOfBoundsTest() {
        new Fields("foo").fieldIndex("baz");
    }

    @Test
    public void containsTest() {
        Fields fields = new Fields("foo", "bar");
        Assert.assertTrue(fields.contains("foo"));
        Assert.assertTrue(fields.contains("bar"));
        Assert.assertFalse(fields.contains("baz"));
    }

    @Test
    public void toListTest() {
        Fields fields = new Fields("foo", "bar");
        List<String> fieldList = fields.toList();
        Assert.assertEquals(fieldList.size(), 2);
        Assert.assertEquals(fieldList.get(0), "foo");
        Assert.assertEquals(fieldList.get(1), "bar");
    }

    @Test
    public void toIteratorTest() {
        Fields fields = new Fields("foo", "bar");
        Iterator<String> fieldIter = fields.iterator();

        Assert.assertTrue(
            "First item is foo",
            fieldIter.hasNext());
        Assert.assertEquals(fieldIter.next(), "foo");

        Assert.assertTrue(
            "Second item is bar",
            fieldIter.hasNext());
        Assert.assertEquals(fieldIter.next(), "bar");

        Assert.assertFalse(
            "At end. hasNext should return false",
            fieldIter.hasNext());
    }

    @Test
    public void selectTest() {
        Fields fields = new Fields("foo", "bar");
        List<Object> second = Arrays.asList(new Object[]{ "b" });
        List<Object> tuple = Arrays.asList(new Object[]{ "a", "b", "c" });
        List<Object> pickSecond = fields.select(new Fields("bar"), tuple);
        Assert.assertTrue(pickSecond.equals(second));

        List<Object> secondAndFirst = Arrays.asList(new Object[]{ "b", "a" });
        List<Object> pickSecondAndFirst = fields.select(new Fields("bar", "foo"), tuple);
        Assert.assertTrue(pickSecondAndFirst.equals(secondAndFirst));
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectingUnknownFieldThrowsTest() {
        Fields fields = new Fields("foo", "bar");
        fields.select(new Fields("bar", "baz"), Arrays.asList(new Object[]{ "a", "b", "c" }));
    }
}
