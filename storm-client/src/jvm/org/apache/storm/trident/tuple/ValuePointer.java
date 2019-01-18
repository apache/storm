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

package org.apache.storm.trident.tuple;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.tuple.Fields;

public class ValuePointer {
    public int delegateIndex;
    protected int index;
    protected String field;

    public ValuePointer(int delegateIndex, int index, String field) {
        this.delegateIndex = delegateIndex;
        this.index = index;
        this.field = field;
    }

    public static Map<String, ValuePointer> buildFieldIndex(ValuePointer[] pointers) {
        Map<String, ValuePointer> ret = new HashMap<String, ValuePointer>();
        for (ValuePointer ptr : pointers) {
            ret.put(ptr.field, ptr);
        }
        return ret;
    }

    public static ValuePointer[] buildIndex(Fields fieldsOrder, Map<String, ValuePointer> pointers) {
        if (fieldsOrder.size() != pointers.size()) {
            throw new IllegalArgumentException("Fields order must be same length as pointers map");
        }
        ValuePointer[] ret = new ValuePointer[pointers.size()];
        for (int i = 0; i < fieldsOrder.size(); i++) {
            ret[i] = pointers.get(fieldsOrder.get(i));
        }
        return ret;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
