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

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;

/**
 * Wraps a {@link FileWriterFactory} and remembers every data writer it hands out, so the state can
 * ask how many bytes the currently buffered window has produced.
 *
 * <p>The figure is an <em>estimate</em>: {@link DataWriter#length()} reflects what the
 * underlying format has flushed, and columnar formats such as Parquet keep a sizeable in-memory
 * buffer before writing a row group. It therefore under-reports until a file is closed, which for
 * a commit threshold only means committing slightly later than the configured size.
 *
 * <p>Closed writers are kept in the list on purpose: a rolled-over file still counts towards the
 * bytes accumulated since the last commit. {@link #reset()} drops them when the window is flushed.
 */
class CountingFileWriterFactory implements FileWriterFactory<Record> {

    private final FileWriterFactory<Record> delegate;
    private final List<DataWriter<Record>> dataWriters = new ArrayList<>();

    CountingFileWriterFactory(FileWriterFactory<Record> delegate) {
        this.delegate = delegate;
    }

    /** Bytes written by every writer created since the last {@link #reset()}. */
    long estimatedBytes() {
        long total = 0L;
        for (DataWriter<Record> dataWriter : dataWriters) {
            total += dataWriter.length();
        }
        return total;
    }

    /** Forget the writers of the window that was just committed or aborted. */
    void reset() {
        dataWriters.clear();
    }

    @Override
    public DataWriter<Record> newDataWriter(EncryptedOutputFile file, PartitionSpec spec,
                                            StructLike partition) {
        DataWriter<Record> dataWriter = delegate.newDataWriter(file, spec, partition);
        dataWriters.add(dataWriter);
        return dataWriter;
    }

    @Override
    public EqualityDeleteWriter<Record> newEqualityDeleteWriter(EncryptedOutputFile file,
                                                                PartitionSpec spec, StructLike partition) {
        // The sink is append-only; delete writers are never requested.
        return delegate.newEqualityDeleteWriter(file, spec, partition);
    }

    @Override
    public PositionDeleteWriter<Record> newPositionDeleteWriter(EncryptedOutputFile file,
                                                                PartitionSpec spec, StructLike partition) {
        return delegate.newPositionDeleteWriter(file, spec, partition);
    }
}
