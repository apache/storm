/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.Writer;
import org.apache.storm.hdfs.bolt.rotation.ClosingFilesPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

public abstract class AbstractHDFSWriter implements Writer {
    protected long lastUsedTime;
    protected long offset;
    protected boolean needsRotation;
    protected final Path filePath;
    protected final FileRotationPolicy rotationPolicy;
    ClosingFilesPolicy closingFilesPolicy;

    /**
     * Abstract constructor with FileRotationPolicy.
     * @param policy File Rotation Policy
     * @param path Path
     */
    public AbstractHDFSWriter(FileRotationPolicy policy, Path path) {
        //This must be defensively copied, because a bolt probably has only one rotation policy object
        this.rotationPolicy = policy.copy();
        this.filePath = path;
    }

    /**
     * Abstract constructor with ClosingFilesPolicy.
     * @param policy ClosingFilesPolicy
     */
    public AbstractHDFSWriter withClosingFilesPolicy(ClosingFilesPolicy policy) {
        this.closingFilesPolicy = policy;
        return this;
    }

    /**
     * Method to write data to hdfs.
     * @param tuple Tuple value to write.
     */
    public final long write(Tuple tuple) throws IOException {
        doWrite(tuple);
        this.needsRotation = rotationPolicy.mark(tuple, offset);

        return this.offset;
    }

    public final void sync() throws IOException {
        doSync();
    }

    public final void close() throws IOException {
        doClose();
    }

    public final void updateClosingPolicy() {
        this.needsRotation = closingFilesPolicy.closeWriter();
    }

    public final void resetClosingPolicy() {
        closingFilesPolicy.reset();
    }

    public boolean needsRotation() {
        return needsRotation;
    }

    public Path getFilePath() {
        return this.filePath;
    }

    protected abstract void doWrite(Tuple tuple) throws IOException;

    protected abstract void doSync() throws IOException;

    protected abstract void doClose() throws IOException;

}
