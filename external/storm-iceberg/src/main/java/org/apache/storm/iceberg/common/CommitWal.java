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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.util.JsonUtil;

/**
 * Write-ahead log of Iceberg commits that have been prepared but not yet made visible.
 *
 * <p>An entry is written after the batch's data files are durable and before the Iceberg commit
 * that references them. It therefore protects only the reference, not the data: a crash before the
 * entry exists leaves orphan data files, a crash after it exists is recoverable, because the entry
 * names the files and carries the commit id that
 * {@link IcebergCommitter} records in the resulting snapshot's summary.
 *
 * <p>Entries live under the table's metadata location, not on worker-local disk, so a task
 * relaunched on another host still finds the commits it left behind.
 */
public final class CommitWal {

    static final String WAL_DIR = "_storm_wal";
    private static final String COMMIT_ID = "commit-id";
    private static final String CREATED_AT_MS = "created-at-ms";
    private static final String DATA_FILES = "data-files";

    private final Table table;
    private final FileIO io;
    private final String prefix;

    /**
     * Entries are keyed by component and task index rather than by global task id: task ids are
     * assigned per submission and shift when the topology's structure changes, which would strand
     * an entry under an id nobody reads again.
     */
    public CommitWal(Table table, String topologyName, String componentId, int taskIndex) {
        this.table = table;
        this.io = table.io();
        String location = table.location();
        while (location.endsWith("/")) {
            location = location.substring(0, location.length() - 1);
        }
        this.prefix = location + "/metadata/" + WAL_DIR + "/" + topologyName
            + "/" + componentId + "/" + taskIndex;
    }

    /** Record the files of one prepared commit, returning the entry that identifies it. */
    public WalEntry write(List<DataFile> dataFiles) {
        String commitId = UUID.randomUUID().toString();
        long createdAtMs = System.currentTimeMillis();
        // The creation time goes in the file name as well as the body, so listing the WAL yields
        // it without opening anything.
        String location = prefix + "/" + createdAtMs + "-" + commitId + ".json";
        OutputFile outputFile = io.newOutputFile(location);
        try (OutputStream out = outputFile.create();
             JsonGenerator json = JsonUtil.factory()
                 .createGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            json.writeStartObject();
            json.writeStringField(COMMIT_ID, commitId);
            json.writeNumberField(CREATED_AT_MS, createdAtMs);
            json.writeFieldName(DATA_FILES);
            DataFileCodec.writeArray(json, dataFiles, table);
            json.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing Iceberg commit WAL entry " + location, e);
        }
        return new WalEntry(commitId, location, createdAtMs);
    }

    /** Entries left behind by this topology and task, oldest first. */
    public List<WalEntry> listPending() {
        if (!(io instanceof SupportsPrefixOperations)) {
            throw new UnsupportedOperationException(
                "Iceberg FileIO " + io.getClass().getName() + " cannot list the commit WAL; "
                    + "use a FileIO supporting prefix operations");
        }
        List<WalEntry> entries = new ArrayList<>();
        try {
            ((SupportsPrefixOperations) io).listPrefix(prefix + "/")
                .forEach(fileInfo -> {
                    String location = fileInfo.location();
                    if (location.endsWith(".json")) {
                        entries.add(new WalEntry(commitIdOf(location), location, createdAtMsOf(location)));
                    }
                });
        } catch (UncheckedIOException e) {
            // A task that has never committed has no WAL directory. On a hierarchical file system
            // listing it raises FileNotFoundException; on object stores the prefix is simply empty.
            if (!(e.getCause() instanceof FileNotFoundException)) {
                throw e;
            }
            return List.of();
        }
        entries.sort(Comparator.comparing(WalEntry::location));
        return entries;
    }

    /** The data files named by an entry, resolved against the spec they were written with. */
    public List<DataFile> read(WalEntry entry) {
        InputFile inputFile = io.newInputFile(entry.location());
        try (InputStream in = inputFile.newStream()) {
            JsonNode root = JsonUtil.mapper().readTree(in);
            return DataFileCodec.readArray(root.get(DATA_FILES), table.specs());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading Iceberg commit WAL entry " + entry.location(), e);
        }
    }

    /** Drop an entry whose commit is known to be visible. */
    public void delete(WalEntry entry) {
        io.deleteFile(entry.location());
    }

    private static String commitIdOf(String location) {
        String name = fileName(location);
        return name.substring(name.indexOf('-') + 1);
    }

    private static long createdAtMsOf(String location) {
        String name = fileName(location);
        return Long.parseLong(name.substring(0, name.indexOf('-')));
    }

    private static String fileName(String location) {
        String name = location.substring(location.lastIndexOf('/') + 1);
        return name.substring(0, name.length() - ".json".length());
    }

    /**
     * A prepared commit: its id, as recorded in the snapshot summary, where it is logged, and when
     * it was logged — which bounds how far back recovery has to look for its snapshot.
     */
    public record WalEntry(String commitId, String location, long createdAtMs) {
    }
}
