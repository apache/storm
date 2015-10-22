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

import com.google.common.base.Joiner;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.storm.runtime.AbstractValuesProcessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PlanCompiler {
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String PACKAGE_NAME = "org.apache.storm.sql.generated";
  private static final String PROLOGUE = NEW_LINE_JOINER.join(
      "// GENERATED CODE", "package " + PACKAGE_NAME + ";", "",
      "import java.util.Iterator;", "import java.util.Map;",
      "import backtype.storm.tuple.Values;",
      "import org.apache.storm.sql.storm.runtime.AbstractValuesProcessor;", "",
      "public final class Processor extends AbstractValuesProcessor {", "",
      "@Override",
      "protected Iterator<Values>[] getDataSource() { return _datasources; }","");
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  private String generateJavaSource(RelNode root) throws Exception {
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      RelNodeCompiler compiler = new RelNodeCompiler(pw, typeFactory);
      printPrologue(pw);
      compiler.traverse(root);
      printTableReference(pw, compiler.getReferredTables());
      printEpilogue(pw, root);
    }
    return sw.toString();
  }

  private void printTableReference(PrintWriter pw, Set<String> referredTables) {
    Map<String, Integer> ids = new HashMap<>();
    for (String s : referredTables) {
      pw.print(String.format("  private static final int TABLE_%s = %d;\n", s,
                             ids.size()));
      ids.put(s, ids.size());
    }

    pw.print("  @SuppressWarnings(\"unchecked\")\n");
    pw.print(String.format(
        "  private final Iterator<Values>[] _datasources = new Iterator[%d];\n",
        referredTables.size()));
    pw.print(
        "  @Override public void initialize(Map<String, Iterator<Values>> " +
            "data) {\n");
    for (String s : referredTables) {
      String escaped = CompilerUtil.escapeJavaString(s, true);
      String r = NEW_LINE_JOINER.join("  if (!data.containsKey(%1$s))",
                                      "    throw new RuntimeException(\"Cannot find table \" + %1$s);",
                                      "  _datasources[%2$d] = data.get(%1$s);");
      pw.print(String.format(r, escaped, ids.get(s)));
    }
    pw.print("}\n");
  }

  public AbstractValuesProcessor compile(RelNode plan) throws Exception {
    String javaCode = generateJavaSource(plan);
    ClassLoader cl = new CompilingClassLoader(getClass().getClassLoader(),
                                              PACKAGE_NAME + ".Processor",
                                              javaCode, null);
    AbstractValuesProcessor processor = (AbstractValuesProcessor) cl.loadClass(
        PACKAGE_NAME + ".Processor").newInstance();
    return processor;
  }

  private static void printEpilogue(
      PrintWriter pw, RelNode root) throws Exception {
    pw.print("  @Override public Values next() {\n");
    MainBodyCompiler compiler = new MainBodyCompiler(pw);
    compiler.traverse(root);
    pw.print(String.format("  return t%d;\n", root.getId()));
    pw.print("  }\n");
    pw.print("}\n");
  }

  private static void printPrologue(PrintWriter pw) {
    pw.append(PROLOGUE);
  }

  private static class MainBodyCompiler extends PostOrderRelNodeVisitor<Void> {
    private final PrintWriter pw;

    private MainBodyCompiler(PrintWriter pw) {
      this.pw = pw;
    }

    @Override
    Void defaultValue(RelNode n) {
      String params;
      if (n instanceof TableScan) {
        params = "";
      } else {
        ArrayList<String> inputIds = new ArrayList<>();
        for (RelNode i : n.getInputs()) {
          inputIds.add("t" + i.getId());
        }
        params = Joiner.on(",").join(inputIds);
      }
      String l = String.format("  Values t%d = %s(%s);\n", n.getId(),
                               RelNodeCompiler.getFunctionName(n), params);
      pw.print(l);
      return null;
    }
  }
}
