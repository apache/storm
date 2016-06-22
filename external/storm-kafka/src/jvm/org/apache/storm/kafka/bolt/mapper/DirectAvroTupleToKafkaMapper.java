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
package org.apache.storm.kafka.bolt.mapper;

import java.io.IOException;

import org.apache.avro.generic.GenericContainer;
import org.apache.storm.avro.DefaultDirectAvroSerializer;
import org.apache.storm.avro.DirectAvroSerializer;
import org.apache.storm.tuple.Tuple;

public class DirectAvroTupleToKafkaMapper<K> extends FieldNameBasedTupleToKafkaMapper<K,byte[]> {

    DirectAvroSerializer serializer = new DefaultDirectAvroSerializer();

    public DirectAvroTupleToKafkaMapper() {
        super();
    }

    public DirectAvroTupleToKafkaMapper(String boltKeyField, String boltMessageField) {
        super(boltKeyField, boltMessageField);
    }

    @Override
    public byte[] getMessageFromTuple(Tuple tuple) {
        Object obj = tuple.getValueByField(boltMessageField);
        try {
            return obj == null ? null : serializer.serialize((GenericContainer)obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
