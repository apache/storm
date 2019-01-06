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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import org.apache.storm.generated.SettableBlobMeta;

public class LocalFsBlobStoreFile extends BlobStoreFile {

    private final String _key;
    private final boolean _isTmp;
    private final Path _path;
    private final boolean _mustBeNew;
    private Long _modTime = null;
    private SettableBlobMeta meta;

    public LocalFsBlobStoreFile(Path base, String name) {
        if (BlobStoreFile.BLOBSTORE_DATA_FILE.equals(name)) {
            _isTmp = false;
        } else {
            Matcher m = TMP_NAME_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new IllegalArgumentException("File name does not match '" + name + "' !~ " + TMP_NAME_PATTERN);
            }
            _isTmp = true;
        }
        _key = base.getFileName().toString();
        _path = base.resolve(name);
        _mustBeNew = false;
    }

    public LocalFsBlobStoreFile(Path base, boolean isTmp, boolean mustBeNew) {
        _key = base.getFileName().toString();
        _isTmp = isTmp;
        _mustBeNew = mustBeNew;
        if (_isTmp) {
            _path = base.resolve(System.currentTimeMillis() + TMP_EXT);
        } else {
            _path = base.resolve(BlobStoreFile.BLOBSTORE_DATA_FILE);
        }
    }

    @Override
    public void delete() throws IOException {
        Files.delete(_path);
    }

    @Override
    public boolean isTmp() {
        return _isTmp;
    }

    @Override
    public String getKey() {
        return _key;
    }

    @Override
    public long getModTime() throws IOException {
        if (_modTime == null) {
            _modTime = Files.getLastModifiedTime(_path).toMillis();
        }
        return _modTime;
    }

    @Override
    public InputStream getInputStream() throws NoSuchFileException, IOException {
        if (isTmp()) {
            throw new IllegalStateException("Cannot read from a temporary part file.");
        }
        return Files.newInputStream(_path);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }
        try {
            Files.createFile(_path);
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            //Try to create the parent directory, may not work
            Files.createDirectories(_path.getParent());
            Files.createFile(_path);
        }
        return Files.newOutputStream(_path);
    }

    @Override
    public void commit() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }

        Path dest = _path.getParent().resolve(BlobStoreFile.BLOBSTORE_DATA_FILE);
        if (_mustBeNew) {
            Files.move(_path, dest, StandardCopyOption.ATOMIC_MOVE);
        } else {
            Files.move(_path, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
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
        return _path + ":" + (_isTmp ? "tmp" : BlobStoreFile.BLOBSTORE_DATA_FILE) + ":" + _key;
    }

    @Override
    public long getFileLength() throws IOException {
        return Files.size(_path);
    }
}

