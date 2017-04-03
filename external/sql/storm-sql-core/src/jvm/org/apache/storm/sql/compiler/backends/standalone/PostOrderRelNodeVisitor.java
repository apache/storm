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
package org.apache.storm.sql.compiler.backends.standalone;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.stream.Delta;

import java.util.ArrayList;
import java.util.List;

public abstract class PostOrderRelNodeVisitor<T> {
  public final T traverse(RelNode n) throws Exception {
    List<T> inputStreams = new ArrayList<>();
    for (RelNode input : n.getInputs()) {
      inputStreams.add(traverse(input));
    }

    if (n instanceof Aggregate) {
      return visitAggregate((Aggregate) n, inputStreams);
    } else if (n instanceof Calc) {
      return visitCalc((Calc) n, inputStreams);
    } else if (n instanceof Collect) {
      return visitCollect((Collect) n, inputStreams);
    } else if (n instanceof Correlate) {
      return visitCorrelate((Correlate) n, inputStreams);
    } else if (n instanceof Delta) {
      return visitDelta((Delta) n, inputStreams);
    } else if (n instanceof Exchange) {
      return visitExchange((Exchange) n, inputStreams);
    } else if (n instanceof Project) {
      return visitProject((Project) n, inputStreams);
    } else if (n instanceof Filter) {
      return visitFilter((Filter) n, inputStreams);
    } else if (n instanceof Sample) {
      return visitSample((Sample) n, inputStreams);
    } else if (n instanceof Sort) {
      return visitSort((Sort) n, inputStreams);
    } else if (n instanceof TableModify) {
      return visitTableModify((TableModify) n, inputStreams);
    } else if (n instanceof TableScan) {
      return visitTableScan((TableScan) n, inputStreams);
    } else if (n instanceof Uncollect) {
      return visitUncollect((Uncollect) n, inputStreams);
    } else if (n instanceof Window) {
      return visitWindow((Window) n, inputStreams);
    } else if (n instanceof Join) {
      return visitJoin((Join) n, inputStreams);
    } else {
      return defaultValue(n, inputStreams);
    }
  }

  public T visitAggregate(Aggregate aggregate, List<T> inputStreams) throws Exception {
    return defaultValue(aggregate, inputStreams);
  }

  public T visitCalc(Calc calc, List<T> inputStreams) throws Exception {
    return defaultValue(calc, inputStreams);
  }

  public T visitCollect(Collect collect, List<T> inputStreams) throws Exception {
    return defaultValue(collect, inputStreams);
  }

  public T visitCorrelate(Correlate correlate, List<T> inputStreams) throws Exception {
    return defaultValue(correlate, inputStreams);
  }

  public T visitDelta(Delta delta, List<T> inputStreams) throws Exception {
    return defaultValue(delta, inputStreams);
  }

  public T visitExchange(Exchange exchange, List<T> inputStreams) throws Exception {
    return defaultValue(exchange, inputStreams);
  }

  public T visitProject(Project project, List<T> inputStreams) throws Exception {
    return defaultValue(project, inputStreams);
  }

  public T visitFilter(Filter filter, List<T> inputStreams) throws Exception {
    return defaultValue(filter, inputStreams);
  }

  public T visitSample(Sample sample, List<T> inputStreams) throws Exception {
    return defaultValue(sample, inputStreams);
  }

  public T visitSort(Sort sort, List<T> inputStreams) throws Exception {
    return defaultValue(sort, inputStreams);
  }

  public T visitTableModify(TableModify modify, List<T> inputStreams) throws Exception {
    return defaultValue(modify, inputStreams);
  }

  public T visitTableScan(TableScan scan, List<T> inputStreams) throws Exception {
    return defaultValue(scan, inputStreams);
  }

  public T visitUncollect(Uncollect uncollect, List<T> inputStreams) throws Exception {
    return defaultValue(uncollect, inputStreams);
  }

  public T visitWindow(Window window, List<T> inputStreams) throws Exception {
    return defaultValue(window, inputStreams);
  }

  public T visitJoin(Join join, List<T> inputStreams) throws Exception {
    return defaultValue(join, inputStreams);
  }

  public T defaultValue(RelNode n, List<T> inputStreams) {
    return null;
  }
}
