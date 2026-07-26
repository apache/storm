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

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Serializable configuration for {@link IcebergState}.
 *
 * <p>The catalog properties are passed verbatim to
 * {@code CatalogUtil.buildIcebergCatalog(...)}, so any Iceberg catalog (hive, hadoop, rest,
 * glue, nessie, ...) can be configured with the same property keys documented by Iceberg.
 */
public class IcebergOptions implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Map<String, String> catalogProperties;
    private final String tableIdentifier;
    private final RecordMapper recordMapper;
    private final FileFormat fileFormat;
    private final Long targetFileSizeBytes;
    private final Schema autoCreateSchema;
    private final PartitionSpec autoCreateSpec;
    private final Long commitIntervalBytes;
    private final Long commitIntervalMillis;

    private IcebergOptions(Builder builder) {
        this.catalogProperties = builder.catalogProperties;
        this.tableIdentifier = builder.tableIdentifier;
        this.recordMapper = builder.recordMapper;
        this.fileFormat = builder.fileFormat;
        this.targetFileSizeBytes = builder.targetFileSizeBytes;
        this.autoCreateSchema = builder.autoCreateSchema;
        this.autoCreateSpec = builder.autoCreateSpec;
        this.commitIntervalBytes = builder.commitIntervalBytes;
        this.commitIntervalMillis = builder.commitIntervalMillis;
    }

    public Map<String, String> getCatalogProperties() {
        return catalogProperties;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public RecordMapper getRecordMapper() {
        return recordMapper;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    public Long getTargetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    public Long getCommitIntervalBytes() {
        return commitIntervalBytes;
    }

    public Long getCommitIntervalMillis() {
        return commitIntervalMillis;
    }

    /** True when batches are buffered across commits instead of committed one by one. */
    public boolean isCommitBatchingEnabled() {
        return commitIntervalBytes != null || commitIntervalMillis != null;
    }

    public Schema getAutoCreateSchema() {
        return autoCreateSchema;
    }

    public PartitionSpec getAutoCreateSpec() {
        return autoCreateSpec;
    }

    public static class Builder {
        private Map<String, String> catalogProperties;
        private String tableIdentifier;
        private RecordMapper recordMapper = new FieldNameRecordMapper();
        private FileFormat fileFormat = FileFormat.PARQUET;
        private Long targetFileSizeBytes;
        private Schema autoCreateSchema;
        private PartitionSpec autoCreateSpec;
        private Long commitIntervalBytes;
        private Long commitIntervalMillis;

        public Builder withCatalogProperties(Map<String, String> properties) {
            this.catalogProperties = properties == null ? null : new HashMap<>(properties);
            return this;
        }

        public Builder withTable(String identifier) {
            this.tableIdentifier = identifier;
            return this;
        }

        public Builder withRecordMapper(RecordMapper mapper) {
            this.recordMapper = mapper;
            return this;
        }

        public Builder withFileFormat(FileFormat format) {
            this.fileFormat = format;
            return this;
        }

        public Builder withTargetFileSizeBytes(long bytes) {
            this.targetFileSizeBytes = bytes;
            return this;
        }

        /**
         * Create the table on first use when it does not exist, with the given schema and
         * partition spec. A null spec means unpartitioned.
         */
        public Builder withAutoCreate(Schema schema, PartitionSpec spec) {
            this.autoCreateSchema = schema;
            this.autoCreateSpec = spec;
            return this;
        }

        /**
         * Buffer batches until roughly this many bytes have been written, then commit them all in
         * one Iceberg transaction. Without it every Trident batch produces its own commit and
         * snapshot.
         *
         * <p><strong>This weakens the delivery guarantee.</strong> Trident considers a batch
         * delivered as soon as {@code commit()} returns, so batches buffered by this setting are
         * lost if the worker dies before the flush. Leave it unset to keep exactly-once.
         */
        public Builder withCommitIntervalBytes(long bytes) {
            this.commitIntervalBytes = bytes;
            return this;
        }

        /**
         * Flush the buffered batches once the oldest one is older than this, evaluated when the
         * next batch is committed. A stalled stream therefore leaves the last window uncommitted
         * until data resumes. Carries the same durability caveat as
         * {@link #withCommitIntervalBytes(long)}.
         */
        public Builder withCommitIntervalMillis(long millis) {
            this.commitIntervalMillis = millis;
            return this;
        }

        public IcebergOptions build() {
            if (catalogProperties == null || catalogProperties.isEmpty()) {
                throw new IllegalStateException("Catalog properties must be specified.");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalStateException("Table identifier must be specified.");
            }
            if (recordMapper == null) {
                throw new IllegalStateException("RecordMapper must not be null.");
            }
            if (fileFormat == null) {
                throw new IllegalStateException("FileFormat must not be null.");
            }
            if (targetFileSizeBytes != null && targetFileSizeBytes <= 0) {
                throw new IllegalStateException("Target file size must be positive.");
            }
            if (commitIntervalBytes != null && commitIntervalBytes <= 0) {
                throw new IllegalStateException("Commit interval bytes must be positive.");
            }
            if (commitIntervalMillis != null && commitIntervalMillis <= 0) {
                throw new IllegalStateException("Commit interval millis must be positive.");
            }
            return new IcebergOptions(this);
        }
    }
}
