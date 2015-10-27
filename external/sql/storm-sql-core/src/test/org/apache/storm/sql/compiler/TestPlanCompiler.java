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
package org.apache.storm.sql.compiler;

import backtype.storm.tuple.Values;
import org.apache.storm.sql.storm.ValueIterator;
import org.apache.storm.sql.storm.runtime.AbstractValuesProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestPlanCompiler {
  private static final List<Values> INPUTS;

  static {
    ArrayList<Values> records = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      records.add(new Values(i));
    }
    INPUTS = Collections.unmodifiableList(records);
  }

  @Test
  public void testCompile() throws Exception {
    String sql = "SELECT ID + 1 FROM FOO WHERE ID > 2";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler();
    AbstractValuesProcessor proc = compiler.compile(state.tree);
    Map<String, Iterator<Values>> data = new HashMap<>();
    data.put("FOO", INPUTS.iterator());
    proc.initialize(data);
    ValueIterator v = new ValueIterator(proc);
    List<Integer> results = new ArrayList<>();
    while(v.hasNext()) {
      results.add((Integer) v.next().get(0));
    }
    assertEquals(2, results.size());
    assertEquals(4, results.get(0).intValue());
    assertEquals(5, results.get(1).intValue());
  }

  @Test
  public void testLogicalExpr() throws Exception {
    String sql = "SELECT ID > 0 OR ID < 1, ID > 0 AND ID < 1, " +
        "NOT (ID > 0 AND ID < 1) FROM FOO WHERE ID > 0 AND ID < 2";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler();
    AbstractValuesProcessor proc = compiler.compile(state.tree);
    Map<String, Iterator<Values>> data = new HashMap<>();
    data.put("FOO", INPUTS.iterator());
    proc.initialize(data);
    ValueIterator v = new ValueIterator(proc);
    assertTrue(v.hasNext());
    Values val = v.next();
    assertEquals(true, val.get(0));
    assertEquals(false, val.get(1));
    assertEquals(true, val.get(2));
  }

}
