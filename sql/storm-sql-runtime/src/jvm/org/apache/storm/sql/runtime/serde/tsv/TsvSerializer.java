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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.storm.sql.runtime.IOutputSerializer;

/**
 * TsvSerializer uses a simple delimited format implemention by splitting string,
 * and it supports user defined delimiter.
 */
public class TsvSerializer implements IOutputSerializer, Serializable {
    private final List<String> fields; //reserved for future
    private final char delimiter;

    public TsvSerializer(List<String> fields, char delimiter) {
        this.fields = fields;
        this.delimiter = delimiter;
    }

    @Override
    public ByteBuffer write(List<Object> data, ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder(512); // 512: for most scenes to avoid inner array resizing
        for (int i = 0; i < data.size(); i++) {
            Object o = data.get(i);
            if (i == 0) {
                sb.append(o);
            } else {
                sb.append(delimiter);
                sb.append(o);
            }
        }
        return ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
    }
}
