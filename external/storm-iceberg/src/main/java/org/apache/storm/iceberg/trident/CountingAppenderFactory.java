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

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;

/**
 * Wraps a {@link FileAppenderFactory} and remembers every writer it hands out, so the state can
 * ask how many bytes the currently buffered window has produced.
 *
 * <p>The figure is an <em>estimate</em>: {@link FileAppender#length()} reflects what the
 * underlying format has flushed, and columnar formats such as Parquet keep a sizeable in-memory
 * buffer before writing a row group. It therefore under-reports until a file is closed, which for
 * a commit threshold only means committing slightly later than the configured size.
 *
 * <p>Closed writers are kept in the list on purpose: a rolled-over file still counts towards the
 * bytes accumulated since the last commit. {@link #reset()} drops them when the window is flushed.
 */
class CountingAppenderFactory implements FileAppenderFactory<Record> {

    private final FileAppenderFactory<Record> delegate;
    private final List<FileAppender<Record>> appenders = new ArrayList<>();
    private final List<DataWriter<Record>> dataWriters = new ArrayList<>();

    CountingAppenderFactory(FileAppenderFactory<Record> delegate) {
        this.delegate = delegate;
    }

    /** Bytes written by every writer created since the last {@link #reset()}. */
    long estimatedBytes() {
        long total = 0L;
        for (FileAppender<Record> appender : appenders) {
            total += appender.length();
        }
        for (DataWriter<Record> dataWriter : dataWriters) {
            total += dataWriter.length();
        }
        return total;
    }

    /** Forget the writers of the window that was just committed or aborted. */
    void reset() {
        appenders.clear();
        dataWriters.clear();
    }

    @Override
    public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat format) {
        FileAppender<Record> appender = delegate.newAppender(outputFile, format);
        appenders.add(appender);
        return appender;
    }

    @Override
    public FileAppender<Record> newAppender(EncryptedOutputFile outputFile, FileFormat format) {
        FileAppender<Record> appender = delegate.newAppender(outputFile, format);
        appenders.add(appender);
        return appender;
    }

    @Override
    public DataWriter<Record> newDataWriter(EncryptedOutputFile file, FileFormat format,
                                            StructLike partition) {
        DataWriter<Record> dataWriter = delegate.newDataWriter(file, format, partition);
        dataWriters.add(dataWriter);
        return dataWriter;
    }

    @Override
    public EqualityDeleteWriter<Record> newEqDeleteWriter(EncryptedOutputFile file,
                                                          FileFormat format, StructLike partition) {
        // The sink is append-only; delete writers are never requested.
        return delegate.newEqDeleteWriter(file, format, partition);
    }

    @Override
    public PositionDeleteWriter<Record> newPosDeleteWriter(EncryptedOutputFile file,
                                                           FileFormat format,
                                                           StructLike partition) {
        return delegate.newPosDeleteWriter(file, format, partition);
    }
}
