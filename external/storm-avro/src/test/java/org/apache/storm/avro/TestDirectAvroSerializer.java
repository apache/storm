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
package org.apache.storm.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestDirectAvroSerializer {
    private static final String schemaString1 = "{\"type\":\"record\"," +
            "\"name\":\"stormtest1\"," +
            "\"fields\":[{\"name\":\"foo1\",\"type\":\"string\"}," +
            "{ \"name\":\"int1\", \"type\":\"int\" }]}";
    private static final Schema schema1;

    DirectAvroSerializer ser = new DefaultDirectAvroSerializer();

    static {
        Schema.Parser parser = new Schema.Parser();
        schema1 = parser.parse(schemaString1);
    }

    @Test
    public void testSchemas() {
        GenericRecord record = new GenericData.Record(schema1);
        record.put("foo1", "xin");
        record.put("int1", 2016);
        try {
            Assert.assertEquals(record, ser.deserialize(ser.serialize(record), schema1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
