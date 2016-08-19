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
package org.apache.storm.sql.compiler.backends.trident;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.stream.Delta;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.trident.topology.TridentTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public abstract class TridentPostOrderRelNodeVisitor {

  protected TridentTopology topology;

  public TridentPostOrderRelNodeVisitor(TridentTopology topology) {
    this.topology = topology;
  }

  public final IAggregatableStream traverse(RelNode n) throws Exception {
    List<IAggregatableStream> inputStreams = new ArrayList<>();
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

  public IAggregatableStream visitAggregate(Aggregate aggregate, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(aggregate, inputStreams);
  }

  public IAggregatableStream visitCalc(Calc calc, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(calc, inputStreams);
  }

  public IAggregatableStream visitCollect(Collect collect, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(collect, inputStreams);
  }

  public IAggregatableStream visitCorrelate(Correlate correlate, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(correlate, inputStreams);
  }

  public IAggregatableStream visitDelta(Delta delta, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(delta, inputStreams);
  }

  public IAggregatableStream visitExchange(Exchange exchange, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(exchange, inputStreams);
  }

  public IAggregatableStream visitProject(Project project, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(project, inputStreams);
  }

  public IAggregatableStream visitFilter(Filter filter, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(filter, inputStreams);
  }

  public IAggregatableStream visitSample(Sample sample, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(sample, inputStreams);
  }

  public IAggregatableStream visitSort(Sort sort, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(sort, inputStreams);
  }

  public IAggregatableStream visitTableModify(TableModify modify, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(modify, inputStreams);
  }

  public IAggregatableStream visitTableScan(TableScan scan, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(scan, inputStreams);
  }

  public IAggregatableStream visitUncollect(Uncollect uncollect, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(uncollect, inputStreams);
  }

  public IAggregatableStream visitWindow(Window window, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(window, inputStreams);
  }

  public IAggregatableStream visitJoin(Join join, List<IAggregatableStream> inputStreams) throws Exception {
    return defaultValue(join, inputStreams);
  }

  public IAggregatableStream defaultValue(RelNode n, List<IAggregatableStream> inputStreams) {
    return null;
  }
}
