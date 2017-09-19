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

package org.apache.storm.sql.runtime.serde.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.storm.spout.Scheme;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * AvroScheme uses generic(without code generation) instead of specific(with code generation) readers.
 */
public class AvroScheme implements Scheme {
    private final String schemaString;
    private final List<String> fieldNames;
    private final CachedSchemas schemas;

    /**
     * AvroScheme Constructor.
     * @param schemaString schema string
     * @param fieldNames field names
     */
    public AvroScheme(String schemaString, List<String> fieldNames) {
        this.schemaString = schemaString;
        this.fieldNames = fieldNames;
        this.schemas = new CachedSchemas();
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        try {
            Schema schema = schemas.getSchema(schemaString);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(Utils.toByteArray(ser), null);
            GenericRecord record = reader.read(null, decoder);

            ArrayList<Object> list = new ArrayList<>(fieldNames.size());
            for (String field : fieldNames) {
                Object value = record.get(field);
                // Avro strings are stored using a special Avro Utf8 type instead of using Java primitives
                list.add(SerdeUtils.convertAvroUtf8(value));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldNames);
    }
}
