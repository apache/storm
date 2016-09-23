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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.tuple.Fields;

public class ByteKeyValueScheme implements KeyValueScheme {

    private final RawScheme rawScheme = new RawScheme();

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        final ArrayList<Object> result = new ArrayList<>(2);
        result.addAll(rawScheme.deserialize(key));
        result.addAll(rawScheme.deserialize(value));
        return result;
    }

    @Override
    public List<Object> deserialize(ByteBuffer value) {
        return rawScheme.deserialize(value);
    }


    @Override
    public Fields getOutputFields() {
        return new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE);
    }
}
