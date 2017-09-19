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

import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.storm.sql.runtime.IOutputSerializer;

/**
 * AvroSerializer uses generic(without code generation) instead of specific(with code generation) writers.
 */
public class AvroSerializer implements IOutputSerializer, Serializable {
    private final String schemaString;
    private final List<String> fieldNames;
    private final CachedSchemas schemas;

    /**
     * AvroSerializer Constructor.
     * @param schemaString schema string
     * @param fieldNames field names
     */
    public AvroSerializer(String schemaString, List<String> fieldNames) {
        this.schemaString = schemaString;
        this.fieldNames = fieldNames;
        this.schemas = new CachedSchemas();
    }

    @Override
    public ByteBuffer write(List<Object> data, ByteBuffer buffer) {
        Preconditions.checkArgument(data != null && data.size() == fieldNames.size(), "Invalid schemas");
        try {
            Schema schema = schemas.getSchema(schemaString);
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < fieldNames.size(); i++) {
                record.put(fieldNames.get(i), data.get(i));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return ByteBuffer.wrap(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
