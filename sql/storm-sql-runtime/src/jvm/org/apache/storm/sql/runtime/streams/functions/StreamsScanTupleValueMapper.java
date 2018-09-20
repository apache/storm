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

package org.apache.storm.sql.runtime.streams.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.streams.operations.mappers.TupleValueMapper;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class StreamsScanTupleValueMapper implements TupleValueMapper<Values> {
    private final List<String> fieldNames;

    /**
     * Constructor.
     *
     * @param fieldNames fields to be added to.
     */
    public StreamsScanTupleValueMapper(List<String> fieldNames) {
        // prevent issue when the implementation of fieldNames is not serializable
        // getRowType().getFieldNames() returns Calcite Pair$ which is NOT serializable
        this.fieldNames = new ArrayList<>(fieldNames);
    }

    @Override
    public Values apply(Tuple input) {
        Values values = new Values();
        for (String field : fieldNames) {
            values.add(input.getValueByField(field));
        }
        return values;
    }
}