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
 *
 */

package org.apache.storm.sql.planner.streams;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.storm.sql.compiler.RexNodeToJavaCodeCompiler;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.calcite.DebuggableExecutableExpression;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.calcite.StormDataContext;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.tuple.Values;

public class StreamsPlanCreator {
    private final Map<String, ISqlStreamsDataSource> sources;
    private final JavaTypeFactory typeFactory;
    private final RexNodeToJavaCodeCompiler rexCompiler;
    private final StreamBuilder streamBuilder;
    private final DataContext dataContext;

    private final Deque<Stream<Values>> streamStack = new ArrayDeque<>();
    private final List<CompilingClassLoader> classLoaders = new ArrayList<>();

    public StreamsPlanCreator(Map<String, ISqlStreamsDataSource> sources, RexBuilder rexBuilder) {
        this.sources = sources;
        this.rexCompiler = new RexNodeToJavaCodeCompiler(rexBuilder);
        this.typeFactory = (JavaTypeFactory) rexBuilder.getTypeFactory();

        this.streamBuilder = new StreamBuilder();
        this.dataContext = new StormDataContext();
    }

    public void addStream(Stream<Values> stream) throws Exception {
        push(stream);
    }

    public Stream<Values> pop() {
        return streamStack.pop();
    }

    public Map<String, ISqlStreamsDataSource> getSources() {
        return sources;
    }

    public DataContext getDataContext() {
        return dataContext;
    }

    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public StreamBuilder getStreamBuilder() {
        return streamBuilder;
    }

    public ExecutableExpression createScalarInstance(List<RexNode> nodes, RelDataType inputRowType, String className)
        throws CompilingClassLoader.CompilerException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String expr = rexCompiler.compile(nodes, inputRowType, className);
        CompilingClassLoader classLoader = new CompilingClassLoader(
            getLastClassLoader(), className, expr, null);
        ExecutableExpression instance = (ExecutableExpression) classLoader.loadClass(className).newInstance();
        addClassLoader(classLoader);
        return new DebuggableExecutableExpression(instance, expr);
    }

    public ExecutableExpression createScalarInstance(RexProgram program, String className)
        throws CompilingClassLoader.CompilerException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String expr = rexCompiler.compile(program, className);
        CompilingClassLoader classLoader = new CompilingClassLoader(
            getLastClassLoader(), className, expr, null);
        ExecutableExpression instance = (ExecutableExpression) classLoader.loadClass(className).newInstance();
        addClassLoader(classLoader);
        return new DebuggableExecutableExpression(instance, expr);
    }

    private void push(Stream<Values> stream) {
        streamStack.push(stream);
    }

    public void addClassLoader(CompilingClassLoader compilingClassLoader) {
        this.classLoaders.add(compilingClassLoader);
    }

    public ClassLoader getLastClassLoader() {
        if (this.classLoaders.size() > 0) {
            return this.classLoaders.get(this.classLoaders.size() - 1);
        } else {
            return this.getClass().getClassLoader();
        }
    }

    public List<CompilingClassLoader> getClassLoaders() {
        return classLoaders;
    }
}
