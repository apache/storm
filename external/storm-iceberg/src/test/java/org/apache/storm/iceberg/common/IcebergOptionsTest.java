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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergOptionsTest {

    private static final Map<String, String> CATALOG_PROPS = Map.of("type", "hadoop", "warehouse", "file:///tmp/wh");

    private IcebergOptions.Builder validBuilder() {
        return new IcebergOptions.Builder()
            .withCatalogProperties(CATALOG_PROPS)
            .withTable("db.events");
    }

    @Test
    void buildsWithDefaults() {
        IcebergOptions options = validBuilder().build();

        assertEquals(CATALOG_PROPS, options.getCatalogProperties());
        assertEquals("db.events", options.getTableIdentifier());
        assertInstanceOf(FieldNameRecordMapper.class, options.getRecordMapper());
        assertEquals(FileFormat.PARQUET, options.getFileFormat());
        assertNull(options.getTargetFileSizeBytes());
        assertNull(options.getAutoCreateSchema());
        assertNull(options.getAutoCreateSpec());
    }

    @Test
    void rejectsMissingCatalogProperties() {
        IcebergOptions.Builder builder = new IcebergOptions.Builder().withTable("db.events");
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void rejectsMissingTable() {
        IcebergOptions.Builder builder = new IcebergOptions.Builder().withCatalogProperties(CATALOG_PROPS);
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void rejectsNonPositiveTargetFileSize() {
        assertThrows(IllegalStateException.class, () -> validBuilder().withTargetFileSizeBytes(0).build());
    }

    @Test
    void isJavaSerializableWithAutoCreate() throws IOException {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        IcebergOptions options = validBuilder()
            .withAutoCreate(schema, PartitionSpec.unpartitioned())
            .withTargetFileSizeBytes(1024L)
            .build();

        try (ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream())) {
            out.writeObject(options); // must not throw NotSerializableException
        }
    }

    @Test
    void groupCommitOptionsHaveDefaults() {
        IcebergOptions options = validBuilder().build();

        assertEquals(IcebergOptions.DEFAULT_GROUP_COMMIT_INTERVAL_MILLIS,
            options.getGroupCommitIntervalMillis());
        assertEquals(IcebergOptions.DEFAULT_GROUP_COMMIT_MAX_DATA_FILES,
            options.getGroupCommitMaxDataFiles());
        assertNull(options.getTickIntervalSecs(), "no tick override unless asked for");
    }

    @Test
    void groupCommitOptionsAreOverridable() {
        IcebergOptions options = validBuilder()
            .withGroupCommitIntervalMillis(2000L)
            .withGroupCommitMaxDataFiles(50)
            .withTickIntervalSecs(5)
            .build();

        assertEquals(2000L, options.getGroupCommitIntervalMillis());
        assertEquals(50, options.getGroupCommitMaxDataFiles());
        assertEquals(5, options.getTickIntervalSecs());
    }

    @Test
    void nonPositiveGroupCommitOptionsAreRejected() {
        assertThrows(IllegalStateException.class,
            () -> validBuilder().withGroupCommitIntervalMillis(0L).build());
        assertThrows(IllegalStateException.class,
            () -> validBuilder().withGroupCommitMaxDataFiles(0).build());
        assertThrows(IllegalStateException.class,
            () -> validBuilder().withTickIntervalSecs(0).build());
    }

    @Test
    void aWalNamespaceMustBeOnePathSegment() {
        assertEquals("staging", validBuilder().withWalNamespace("staging").build().getWalNamespace());
        assertNull(validBuilder().build().getWalNamespace(), "unset unless asked for");
        // The namespace is interpolated into the WAL path, so a separator would silently reshape it.
        assertThrows(IllegalStateException.class,
            () -> validBuilder().withWalNamespace("a/b").build());
        assertThrows(IllegalStateException.class,
            () -> validBuilder().withWalNamespace(" ").build());
    }
}
