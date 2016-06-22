/**
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
package org.apache.storm.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.storm.avro.DefaultDirectAvroSerializer;
import org.apache.storm.avro.DirectAvroSerializer;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class AvroScheme implements Scheme {
    public static final String AVRO_SCHEME_KEY = "avro";
    DirectAvroSerializer serializer = new DefaultDirectAvroSerializer();
    Schema schema;

    public AvroScheme(String schemaString) {
        schema = new Schema.Parser().parse(schemaString);
    }

    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            GenericContainer record = null;
            if(byteBuffer.hasArray()) {
                record = serializer.deserialize(byteBuffer.array(), schema);
            } else {
                record = serializer.deserialize(Utils.toByteArray(byteBuffer), schema);
            }
            return new Values(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields(AVRO_SCHEME_KEY);
    }
}
