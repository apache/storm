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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.JsonUtil;

/**
 * Serializes lists of {@link DataFile} to JSON and back.
 *
 * <p>The same representation is used by the commit write-ahead log and by the descriptor tuple the
 * writer bolt hands to the committer bolt, so a file's identity survives both a worker restart and
 * a hop between components. Each file carries its own spec id, which is why decoding needs the
 * table's specs rather than a single spec: one batch may span a partition spec evolution.
 */
public final class DataFileCodec {

    private DataFileCodec() {
    }

    /** Encode files as a JSON array. */
    public static String toJson(List<DataFile> dataFiles, Table table) {
        StringWriter out = new StringWriter();
        try (JsonGenerator json = JsonUtil.factory().createGenerator(out)) {
            writeArray(json, dataFiles, table);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed serializing Iceberg data files", e);
        }
        return out.toString();
    }

    /** Decode a JSON array produced by {@link #toJson}. */
    public static List<DataFile> fromJson(String json, Map<Integer, PartitionSpec> specs) {
        try {
            return readArray(JsonUtil.mapper().readTree(json), specs);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed deserializing Iceberg data files", e);
        }
    }

    /** Write the array into an open generator, for callers embedding it in a larger document. */
    static void writeArray(JsonGenerator json, List<DataFile> dataFiles, Table table)
        throws IOException {
        json.writeStartArray();
        for (DataFile dataFile : dataFiles) {
            ContentFileParser.toJson(dataFile, table.specs().get(dataFile.specId()), json);
        }
        json.writeEndArray();
    }

    /** Read an array node produced by {@link #writeArray}. */
    static List<DataFile> readArray(JsonNode arrayNode, Map<Integer, PartitionSpec> specs) {
        List<DataFile> dataFiles = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            dataFiles.add((DataFile) ContentFileParser.fromJson(node, specs));
        }
        return dataFiles;
    }
}
