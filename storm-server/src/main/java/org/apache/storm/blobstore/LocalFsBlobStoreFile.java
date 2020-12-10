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

package org.apache.storm.blobstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import org.apache.storm.generated.SettableBlobMeta;

public class LocalFsBlobStoreFile extends BlobStoreFile {

    private final String key;
    private final boolean isTmp;
    private final File path;
    private final boolean mustBeNew;
    private SettableBlobMeta meta;

    public LocalFsBlobStoreFile(File base, String name) {
        if (BlobStoreFile.BLOBSTORE_DATA_FILE.equals(name)) {
            isTmp = false;
        } else {
            Matcher m = TMP_NAME_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new IllegalArgumentException("File name does not match '" + name + "' !~ " + TMP_NAME_PATTERN);
            }
            isTmp = true;
        }
        key = base.getName();
        path = new File(base, name);
        mustBeNew = false;
    }

    public LocalFsBlobStoreFile(File base, boolean isTmp, boolean mustBeNew) {
        key = base.getName();
        this.isTmp = isTmp;
        this.mustBeNew = mustBeNew;
        if (this.isTmp) {
            path = new File(base, System.currentTimeMillis() + TMP_EXT);
        } else {
            path = new File(base, BlobStoreFile.BLOBSTORE_DATA_FILE);
        }
    }

    @Override
    public void delete() throws IOException {
        path.delete();
    }

    @Override
    public boolean isTmp() {
        return isTmp;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public long getModTime() throws IOException {
        return path.lastModified();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (isTmp()) {
            throw new IllegalStateException("Cannot read from a temporary part file.");
        }
        return new FileInputStream(path);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }
        boolean success = false;
        try {
            success = path.createNewFile();
        } catch (IOException e) {
            //Try to create the parent directory, may not work
            path.getParentFile().mkdirs();
            success = path.createNewFile();
        }
        if (!success) {
            throw new IOException(path + " already exists");
        }
        return new FileOutputStream(path);
    }

    @Override
    public void commit() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }

        File dest = new File(path.getParentFile(), BlobStoreFile.BLOBSTORE_DATA_FILE);
        if (mustBeNew) {
            Files.move(path.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } else {
            Files.move(path.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void cancel() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }
        delete();
    }

    @Override
    public SettableBlobMeta getMetadata() {
        return meta;
    }

    @Override
    public void setMetadata(SettableBlobMeta meta) {
        this.meta = meta;
    }

    @Override
    public String toString() {
        return path + ":" + (isTmp ? "tmp" : BlobStoreFile.BLOBSTORE_DATA_FILE) + ":" + key;
    }

    @Override
    public long getFileLength() {
        return path.length();
    }
}

