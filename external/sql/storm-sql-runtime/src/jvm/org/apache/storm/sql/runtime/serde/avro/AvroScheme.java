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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroScheme implements Scheme {
  private final String schemaString;
  private final List<String> fieldNames;
  private final CachedSchemas schemas;

  public AvroScheme(String schemaString, List<String> fieldNames) {
    this.schemaString = schemaString;
    this.fieldNames = fieldNames;
    this.schemas = new CachedSchemas();
  }

  @Override
  public List<Object> deserialize(ByteBuffer ser) {
    try {
      Schema schema = schemas.getSchema(schemaString);

      DatumReader<GenericContainer> reader = new GenericDatumReader<>(schema);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(Utils.toByteArray(ser), null);
      GenericRecord record = (GenericRecord) reader.read(null, decoder);

      ArrayList<Object> list = new ArrayList<>(fieldNames.size());
      for (String field : fieldNames) {
        Object value = record.get(field);
        // Avro strings are stored using a special Avro type instead of using Java primitives
        if (value instanceof Utf8) {
          list.add(value.toString());
        } else if (value instanceof Map<?, ?>) {
          // Due to type erasure, generic type parameter can't be generalized for whole map
          Map<Object, Object> map = new HashMap<>();
          for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet()) {
            Object key = entry.getKey();
            Object newKey = key instanceof Utf8 ? key.toString() : key;
            Object val = entry.getValue();
            Object newVal = val instanceof Utf8 ? val.toString() : val;
            map.put(newKey, newVal);
          }
          list.add(map);
        } else {
          list.add(value);
        }
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
