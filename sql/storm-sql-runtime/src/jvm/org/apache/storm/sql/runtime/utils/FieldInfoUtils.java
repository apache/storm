/*
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

package org.apache.storm.sql.runtime.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.sql.runtime.FieldInfo;

public final class FieldInfoUtils {

    public static List<String> getFieldNames(List<FieldInfo> fields) {
        return Lists.transform(fields, new FieldNameExtractor());
    }

    private static class FieldNameExtractor implements Function<FieldInfo, String>, Serializable {
        @Override
        public String apply(FieldInfo fieldInfo) {
            return fieldInfo.name();
        }
    }
}
