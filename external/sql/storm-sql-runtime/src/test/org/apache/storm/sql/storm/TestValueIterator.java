/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.storm;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.sql.storm.runtime.AbstractValuesProcessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestValueIterator {

  private static final List<Values> INPUTS;

  static {
    ArrayList<Values> records = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      records.add(new Values(i));
    }
    INPUTS = Collections.unmodifiableList(records);
  }

  private static Iterator<Values> mockedInput() {
    return INPUTS.iterator();
  }

  private static Iterator<Values>[] getDataSource() {
    @SuppressWarnings("unchecked")
    Iterator<Values>[] v = new Iterator[1];
    v[0] = mockedInput();
    return v;
  }

  @Test
  public void testPassThrough() {
    final Iterator<Values>[] source = getDataSource();
    AbstractValuesProcessor processor = new AbstractValuesProcessor() {
      @Override
      public Values next() {
        return source[0].next();
      }

      @Override
      public void initialize(
          Map<String, Iterator<Values>> data) {

      }

      @Override
      protected Iterator<Values>[] getDataSource() {
        return source;
      }
    };
    ValueIterator it = new ValueIterator(processor);
    ArrayList<Values> records = Lists.newArrayList(it);
    assertEquals(INPUTS.size(), records.size());
  }

  @Test
  public void testFilter() {
    final Iterator<Values>[] source = getDataSource();
    AbstractValuesProcessor processor = new AbstractValuesProcessor() {
      @Override
      public Values next() {
        Values t = source[0].next();
        return (int) t.get(0) < 2 ? t : null;
      }

      @Override
      public void initialize(
          Map<String, Iterator<Values>> data) {

      }

      @Override
      protected Iterator<Values>[] getDataSource() {
        return source;
      }

    };
    ValueIterator it = new ValueIterator(processor);
    ArrayList<Values> records = Lists.newArrayList(it);
    assertEquals(2, records.size());
  }
}
