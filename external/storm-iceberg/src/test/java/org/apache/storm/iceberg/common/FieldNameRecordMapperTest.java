/*
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

package org.apache.storm.iceberg.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.storm.tuple.ITuple;
import org.junit.jupiter.api.Test;

class FieldNameRecordMapperTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "score", Types.DoubleType.get()),
        Types.NestedField.optional(4, "ts", Types.TimestampType.withZone()));

    private final FieldNameRecordMapper mapper = new FieldNameRecordMapper();

    private ITuple mockTuple() {
        ITuple tuple = mock(ITuple.class);
        when(tuple.contains(anyString())).thenReturn(false);
        return tuple;
    }

    private void field(ITuple tuple, String name, Object value) {
        when(tuple.contains(name)).thenReturn(true);
        when(tuple.getValueByField(name)).thenReturn(value);
    }

    @Test
    void mapsFieldsByName() {
        ITuple tuple = mockTuple();
        field(tuple, "id", 42L);
        field(tuple, "name", "storm");
        field(tuple, "score", 0.5d);

        Record record = mapper.map(tuple, SCHEMA);

        assertEquals(42L, record.getField("id"));
        assertEquals("storm", record.getField("name"));
        assertEquals(0.5d, record.getField("score"));
        assertNull(record.getField("ts"));
    }

    @Test
    void widensNumericTypes() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "i", Types.IntegerType.get()),
            Types.NestedField.required(2, "l", Types.LongType.get()),
            Types.NestedField.required(3, "f", Types.FloatType.get()),
            Types.NestedField.required(4, "d", Types.DoubleType.get()));
        ITuple tuple = mockTuple();
        field(tuple, "i", (short) 7);
        field(tuple, "l", 7);
        field(tuple, "f", 7);
        field(tuple, "d", 7.0f);

        Record record = mapper.map(tuple, schema);

        assertEquals(7, record.getField("i"));
        assertEquals(7L, record.getField("l"));
        assertEquals(7.0f, record.getField("f"));
        assertEquals((double) 7.0f, record.getField("d"));
    }

    @Test
    void convertsInstantAndEpochMillisToTimestamptz() {
        Instant instant = Instant.parse("2026-07-12T10:15:30Z");
        ITuple tuple = mockTuple();
        field(tuple, "id", 1L);
        field(tuple, "name", "x");
        field(tuple, "ts", instant);

        Record record = mapper.map(tuple, SCHEMA);
        assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), record.getField("ts"));

        ITuple tuple2 = mockTuple();
        field(tuple2, "id", 1L);
        field(tuple2, "name", "x");
        field(tuple2, "ts", instant.toEpochMilli());

        Record record2 = mapper.map(tuple2, SCHEMA);
        assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), record2.getField("ts"));
    }

    @Test
    void missingRequiredFieldThrows() {
        ITuple tuple = mockTuple();
        field(tuple, "id", 1L);
        // "name" (required) absent from the tuple

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> mapper.map(tuple, SCHEMA));
        assertTrue(e.getMessage().contains("name"));
    }

    @Test
    void nullForRequiredFieldThrows() {
        ITuple tuple = mockTuple();
        field(tuple, "id", 1L);
        field(tuple, "name", null);

        assertThrows(IllegalArgumentException.class, () -> mapper.map(tuple, SCHEMA));
    }

    @Test
    void missingOptionalFieldIsNull() {
        ITuple tuple = mockTuple();
        field(tuple, "id", 1L);
        field(tuple, "name", "x");

        Record record = mapper.map(tuple, SCHEMA);
        assertNull(record.getField("score"));
    }
}
