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
 */

package org.apache.storm.rocketmq;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.storm.Testing;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;

public class TestUtils {
    public static void setFieldValue(Object obj, String fieldName, Object value) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Tuple generateTestTuple(String field1, String field2, Object value1, Object value2) {
        MkTupleParam param = new MkTupleParam();
        param.setFields(field1, field2);
        Tuple testTuple = Testing.testTuple(Arrays.asList(value1, value2), param);
        return testTuple;
    }
}
