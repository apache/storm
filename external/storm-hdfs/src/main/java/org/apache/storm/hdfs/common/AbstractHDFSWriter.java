/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hdfs.common;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.Writer;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public abstract class AbstractHDFSWriter implements Writer {
    protected final Path filePath;
    protected final FileRotationPolicy rotationPolicy;
    protected long lastUsedTime;
    protected long offset;
    protected boolean needsRotation;

    public AbstractHDFSWriter(FileRotationPolicy policy, Path path) {
        //This must be defensively copied, because a bolt probably has only one rotation policy object
        this.rotationPolicy = policy.copy();
        this.filePath = path;
    }

    @Override
    public final long write(Tuple tuple) throws IOException {
        doWrite(tuple);
        this.needsRotation = rotationPolicy.mark(tuple, offset);

        return this.offset;
    }

    @Override
    public final void sync() throws IOException {
        doSync();
    }

    @Override
    public final void close() throws IOException {
        doClose();
    }

    @Override
    public boolean needsRotation() {
        return needsRotation;
    }

    @Override
    public Path getFilePath() {
        return this.filePath;
    }

    protected abstract void doWrite(Tuple tuple) throws IOException;

    protected abstract void doSync() throws IOException;

    protected abstract void doClose() throws IOException;

}
