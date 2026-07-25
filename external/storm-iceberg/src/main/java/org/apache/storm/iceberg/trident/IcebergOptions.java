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

    private IcebergOptions(Builder builder) {
        this.catalogProperties = builder.catalogProperties;
        this.tableIdentifier = builder.tableIdentifier;
        this.recordMapper = builder.recordMapper;
        this.fileFormat = builder.fileFormat;
        this.targetFileSizeBytes = builder.targetFileSizeBytes;
        this.autoCreateSchema = builder.autoCreateSchema;
        this.autoCreateSpec = builder.autoCreateSpec;
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
            return new IcebergOptions(this);
        }
    }
}
