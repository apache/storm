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

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Serializable configuration for the Iceberg sink.
 *
 * <p>The catalog properties are passed verbatim to
 * {@code CatalogUtil.buildIcebergCatalog(...)}, so any Iceberg catalog (hive, hadoop, rest,
 * glue, nessie, ...) can be configured with the same property keys documented by Iceberg.
 */
public class IcebergOptions implements Serializable {

    /** Batch size used when a topology configures no commit threshold of its own. */
    public static final int DEFAULT_COMMIT_INTERVAL_RECORDS = 1000;

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
    private final Integer commitIntervalRecords;

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
        this.commitIntervalRecords = builder.commitIntervalRecords;
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

    public Integer getCommitIntervalRecords() {
        return commitIntervalRecords;
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
        private Integer commitIntervalRecords;

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
         * Close the batch once roughly this many bytes have been written. Sizing batches this way
         * keeps data files near the table's target size and the snapshot count sane.
         *
         * <p>Buffering costs latency and replay volume, not durability: buffered tuples are not
         * acked, so a worker that dies before the commit has them replayed rather than lost.
         */
        public Builder withCommitIntervalBytes(long bytes) {
            this.commitIntervalBytes = bytes;
            return this;
        }

        /**
         * Close the batch once it has been open this long, evaluated when the next tuple arrives.
         * Configure {@link org.apache.storm.Config#TOPOLOGY_TICK_TUPLE_FREQ_SECS} as well to bound
         * the latency of the last batch when the stream stalls.
         */
        public Builder withCommitIntervalMillis(long millis) {
            this.commitIntervalMillis = millis;
            return this;
        }

        /** Close the batch once it holds this many tuples. */
        public Builder withCommitIntervalRecords(int records) {
            this.commitIntervalRecords = records;
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
            if (commitIntervalRecords != null && commitIntervalRecords <= 0) {
                throw new IllegalStateException("Commit interval records must be positive.");
            }
            if (commitIntervalBytes == null && commitIntervalMillis == null && commitIntervalRecords == null) {
                // Without a threshold a batch would stay open until a tick tuple arrived, which
                // costs unbounded latency on a topology that configured none.
                this.commitIntervalRecords = DEFAULT_COMMIT_INTERVAL_RECORDS;
            }
            return new IcebergOptions(this);
        }
    }
}
