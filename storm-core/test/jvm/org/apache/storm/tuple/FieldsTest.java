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
package org.apache.storm.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class FieldsTest {

    @Test
    public void fieldsConstructorDoesNotThrowWithValidArgsTest() {
        Assert.assertEquals(new Fields("foo", "bar").size(), 2);
        Assert.assertEquals(new Fields(new String[] {"foo", "bar"}).size(), 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateFieldsNotAllowedWhenConstructingWithVarArgsTest() {
        new Fields("foo", "bar", "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateFieldsNotAllowedTestWhenConstructingFromListTest() {
        new Fields(new String[] {"foo", "bar", "foo"});
    }

    private Fields getFields() {
        return new Fields("foo", "bar");
    }

    @Test
    public void getDoesNotThrowWithValidIndexTest() {
        Assert.assertEquals(getFields().get(0), "foo");
        Assert.assertEquals(getFields().get(1), "bar");
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getThrowsWhenOutOfBoundsTest() {
        getFields().get(3);
    }

    @Test
    public void fieldIndexTest() {
        Assert.assertEquals(getFields().fieldIndex("foo"), 0);
        Assert.assertEquals(getFields().fieldIndex("bar"), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fieldIndexThrowsWhenOutOfBoundsTest() {
        getFields().fieldIndex("baz");
    }

    @Test
    public void containsTest() {
        Assert.assertTrue(getFields().contains("foo"));
        Assert.assertTrue(getFields().contains("bar"));
        Assert.assertFalse(getFields().contains("baz"));
    }

    @Test
    public void toListTest() {
        List<String> fieldList = getFields().toList();
        Assert.assertEquals(fieldList.size(), 2);
        Assert.assertEquals(fieldList.get(0), "foo");
        Assert.assertEquals(fieldList.get(1), "bar");
    }

    @Test
    public void toIteratorTest() {
        Iterator<String> fieldIter = getFields().iterator();

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
        List<Object> second = Arrays.asList(new Object[]{"b"});
        List<Object> tuple = Arrays.asList(new Object[]{"a", "b", "c"});
        List<Object> pickSecond = getFields().select(new Fields("bar"), tuple);
        Assert.assertTrue(pickSecond.equals(second));

        List<Object> secondAndFirst = Arrays.asList(new Object[]{"b", "a"});
        List<Object> pickSecondAndFirst = getFields().select(new Fields("bar", "foo"), tuple);
        Assert.assertTrue(pickSecondAndFirst.equals(secondAndFirst));
    }

    @Test(expected = NullPointerException.class)
    public void selectingUnknownFieldThrowsTest() {
        getFields().select(new Fields("bar", "baz"), Arrays.asList(new Object[]{"a", "b", "c"}));
    }
}
