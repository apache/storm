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

import java.util.List;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

/**
 * State mapper that maps a tuple to separate state fields.
 */
public class NonTransactionalTupleStateMapper implements StateMapper<ITuple> {

    private final Fields fields;

    public NonTransactionalTupleStateMapper(String... fields) {
        this.fields = new Fields(fields);
    }

    public NonTransactionalTupleStateMapper(Fields fields) {
        this.fields = fields;
    }

    @Override
    public Fields getStateFields() {
        return fields;
    }

    @Override
    public Values toValues(ITuple t) {
        return new Values(t.getValues());
    }

    @Override
    public ITuple fromValues(List<Values> values) {
        if (values == null || values.size() == 0) {
            return null;
        }
        return new SimpleTuple(fields, values.get(0));
    }

    @Override
    public String toString() {
        return String.format("{type: %s, fields: %s}", this.getClass().getSimpleName(), fields);
    }
}
