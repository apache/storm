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

package org.apache.storm.sql.runtime.serde.tsv;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * TsvScheme uses a simple delimited format implemention by splitting string,
 * and it supports user defined delimiter.
 */
public class TsvScheme implements Scheme {
    private final List<String> fieldNames;
    private final char delimiter;

    public TsvScheme(List<String> fieldNames, char delimiter) {
        this.fieldNames = fieldNames;
        this.delimiter = delimiter;
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String data = new String(Utils.toByteArray(ser), StandardCharsets.UTF_8);
        List<String> parts = org.apache.storm.sql.runtime.utils.Utils.split(data, delimiter);
        Preconditions.checkArgument(parts.size() == fieldNames.size(), "Invalid schema");

        ArrayList<Object> list = new ArrayList<>(fieldNames.size());
        list.addAll(parts);
        return list;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldNames);
    }
}
