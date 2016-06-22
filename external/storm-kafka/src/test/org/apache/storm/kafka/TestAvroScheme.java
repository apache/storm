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
package org.apache.storm.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.avro.DefaultDirectAvroSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestAvroScheme {
    private static final String schemaString1 = "{\"type\":\"record\"," +
            "\"name\":\"stormtest1\"," +
            "\"fields\":[{\"name\":\"foo1\",\"type\":\"string\"}," +
            "{ \"name\":\"int1\", \"type\":\"int\" }]}";

    AvroScheme avroSchema = new AvroScheme(schemaString1);

    @Test
    public void testAvroSchema() {
        GenericRecord record = new GenericData.Record(new Schema.Parser().parse(schemaString1));
        record.put("foo1", "xin");
        record.put("int1", 2016);
        ByteBuffer byteBuffer = null;
        try {
            byteBuffer = ByteBuffer.wrap(new DefaultDirectAvroSerializer().serialize(record));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(record, (GenericContainer)avroSchema.deserialize(byteBuffer).get(0));
    }

}
