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

package org.apache.storm.sql.compiler;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeName;

public class StormSqlTypeFactoryImpl extends JavaTypeFactoryImpl {

    public StormSqlTypeFactoryImpl() {
    }

    public StormSqlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public RelDataType toSql(RelDataType type) {
        if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            SqlTypeName sqlTypeName = JavaToSqlTypeConversionRules.instance().lookup(javaType.getJavaClass());
            if (sqlTypeName == null) {
                sqlTypeName = SqlTypeName.ANY;
            }
            return createTypeWithNullability(createSqlType(sqlTypeName), type.isNullable());
        }
        return super.toSql(type);
    }
}
