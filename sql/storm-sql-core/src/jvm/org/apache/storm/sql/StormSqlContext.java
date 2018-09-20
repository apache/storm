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

package org.apache.storm.sql;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.sql.compiler.CompilerUtil;
import org.apache.storm.sql.compiler.StormSqlTypeFactoryImpl;
import org.apache.storm.sql.parser.ColumnConstraint;
import org.apache.storm.sql.parser.ColumnDefinition;
import org.apache.storm.sql.parser.SqlCreateFunction;
import org.apache.storm.sql.parser.SqlCreateTable;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.streams.QueryPlanner;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;

public class StormSqlContext {
    private final JavaTypeFactory typeFactory = new StormSqlTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT);
    private final SchemaPlus schema = Frameworks.createRootSchema(true);
    private boolean hasUdf = false;
    private Map<String, ISqlStreamsDataSource> dataSources = new HashMap<>();

    public void interpretCreateTable(SqlCreateTable n) {
        CompilerUtil.TableBuilderInfo builder = new CompilerUtil.TableBuilderInfo(typeFactory);
        List<FieldInfo> fields = new ArrayList<>();
        for (ColumnDefinition col : n.fieldList()) {
            builder.field(col.name(), col.type(), col.constraint());
            RelDataType dataType = col.type().deriveType(typeFactory);
            Class<?> javaType = (Class<?>) typeFactory.getJavaClass(dataType);
            ColumnConstraint constraint = col.constraint();
            boolean isPrimary = constraint != null && constraint instanceof ColumnConstraint.PrimaryKey;
            fields.add(new FieldInfo(col.name(), javaType, isPrimary));
        }

        if (n.parallelism() != null) {
            builder.parallelismHint(n.parallelism());
        }
        Table table = builder.build();
        schema.add(n.tableName(), table);

        ISqlStreamsDataSource ds = DataSourcesRegistry.constructStreamsDataSource(n.location(), n
            .inputFormatClass(), n.outputFormatClass(), n.properties(), fields);
        if (ds == null) {
            throw new RuntimeException("Failed to find data source for " + n
                .tableName() + " URI: " + n.location());
        } else if (dataSources.containsKey(n.tableName())) {
            throw new RuntimeException("Duplicated definition for table " + n
                .tableName());
        }
        dataSources.put(n.tableName(), ds);
    }

    public void interpretCreateFunction(SqlCreateFunction sqlCreateFunction) throws ClassNotFoundException {
        if (sqlCreateFunction.jarName() != null) {
            throw new UnsupportedOperationException("UDF 'USING JAR' not implemented");
        }
        Method method;
        Function function;
        if ((method = findMethod(sqlCreateFunction.className(), "evaluate")) != null) {
            function = ScalarFunctionImpl.create(method);
        } else if (findMethod(sqlCreateFunction.className(), "add") != null) {
            function = AggregateFunctionImpl.create(Class.forName(sqlCreateFunction.className()));
        } else {
            throw new RuntimeException("Invalid scalar or aggregate function");
        }
        schema.add(sqlCreateFunction.functionName().toUpperCase(), function);
        hasUdf = true;
    }

    public AbstractStreamsProcessor compileSql(String query) throws Exception {
        QueryPlanner planner = new QueryPlanner(schema);
        return planner.compile(dataSources, query);
    }

    public String explain(String query) throws SqlParseException, ValidationException, RelConversionException {
        FrameworkConfig config = buildFrameWorkConfig();
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parse = planner.parse(query);
        SqlNode validate = planner.validate(parse);
        RelNode tree = planner.convert(validate);

        return StormRelUtils.explain(tree, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    public FrameworkConfig buildFrameWorkConfig() {
        if (hasUdf) {
            List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
            sqlOperatorTables.add(SqlStdOperatorTable.instance());
            sqlOperatorTables.add(new CalciteCatalogReader(CalciteSchema.from(schema),
                    Collections.emptyList(), typeFactory, new CalciteConnectionConfigImpl(new Properties())));
            return Frameworks.newConfigBuilder().defaultSchema(schema)
                             .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables)).build();
        } else {
            return Frameworks.newConfigBuilder().defaultSchema(schema).build();
        }
    }

    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public SchemaPlus getSchema() {
        return schema;
    }

    public Map<String, ISqlStreamsDataSource> getDataSources() {
        return dataSources;
    }

    private Method findMethod(String clazzName, String methodName) throws ClassNotFoundException {
        Class<?> clazz = Class.forName(clazzName);
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        return null;
    }

}
