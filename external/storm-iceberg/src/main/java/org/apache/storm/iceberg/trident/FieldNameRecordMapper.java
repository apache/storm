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

package org.apache.storm.iceberg.trident;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Default {@link RecordMapper}: matches tuple fields to Iceberg columns by name.
 *
 * <p>Standard primitive conversions are applied (numeric widening, {@link Instant}/epoch-millis to
 * timestamp types, byte[] to ByteBuffer). Values of other types — including java.time values that
 * already match the Iceberg representation, and struct/list/map values — are passed through as-is.
 * A required column with no tuple value fails fast with {@link IllegalArgumentException}.
 */
public class FieldNameRecordMapper implements RecordMapper {

    private static final long serialVersionUID = 1L;

    @Override
    public Record map(TridentTuple tuple, Schema schema) {
        GenericRecord record = GenericRecord.create(schema);
        for (Types.NestedField field : schema.columns()) {
            Object value = tuple.contains(field.name()) ? tuple.getValueByField(field.name()) : null;
            if (value == null) {
                if (field.isRequired()) {
                    throw new IllegalArgumentException(
                        "Tuple has no value for required Iceberg column '" + field.name() + "'");
                }
                continue;
            }
            record.setField(field.name(), convert(field.type(), value));
        }
        return record;
    }

    private Object convert(Type type, Object value) {
        switch (type.typeId()) {
            case INTEGER:
                return value instanceof Number ? ((Number) value).intValue() : value;
            case LONG:
                return value instanceof Number ? ((Number) value).longValue() : value;
            case FLOAT:
                return value instanceof Number ? ((Number) value).floatValue() : value;
            case DOUBLE:
                return value instanceof Number ? ((Number) value).doubleValue() : value;
            case STRING:
                return value instanceof CharSequence ? value.toString() : value;
            case TIMESTAMP:
                return convertTimestamp((Types.TimestampType) type, value);
            case BINARY:
                return value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) : value;
            default:
                return value;
        }
    }

    private Object convertTimestamp(Types.TimestampType type, Object value) {
        Instant instant;
        if (value instanceof Instant) {
            instant = (Instant) value;
        } else if (value instanceof Date) {
            instant = ((Date) value).toInstant();
        } else if (value instanceof Long) {
            instant = Instant.ofEpochMilli((Long) value);
        } else {
            return value; // already an OffsetDateTime / LocalDateTime
        }
        return type.shouldAdjustToUTC()
            ? OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
            : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
