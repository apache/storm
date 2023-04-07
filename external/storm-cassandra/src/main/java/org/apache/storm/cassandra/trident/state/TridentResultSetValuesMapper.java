/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.trident.state;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.storm.cassandra.query.CQLResultSetValuesMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

public class TridentResultSetValuesMapper implements CQLResultSetValuesMapper {
    private Fields outputDeclaredFields;

    public TridentResultSetValuesMapper(Fields outputDeclaredFields) {
        this.outputDeclaredFields = outputDeclaredFields;
    }

    @Override
    public List<List<Values>> map(CqlSession session, Statement statement, ITuple tuple) {
        List<List<Values>> list = new ArrayList<>();
        List<Values> innerList = new LinkedList<>();
        list.add(innerList);
        ResultSet resultSet = session.execute(statement);
        for (Row row : resultSet) {
            final Values values = new Values();
            for (String field : outputDeclaredFields) {
                if (tuple.contains(field)) {
                    values.add(tuple.getValueByField(field));
                } else {
                    values.add(row.getObject(field));
                }
            }
            innerList.add(values);
        }
        return list;
    }
}
