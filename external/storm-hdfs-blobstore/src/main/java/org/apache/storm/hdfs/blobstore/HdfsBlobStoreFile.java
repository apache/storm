/**
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

package org.apache.storm.hdfs.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.generated.SettableBlobMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsBlobStoreFile extends BlobStoreFile {

    // files are world-wide readable and owner writable
    public static final FsPermission BLOBSTORE_FILE_PERMISSION =
            FsPermission.createImmutable((short) 0644); // rw-r--r--

    private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreFile.class);

    private final String key;
    private final boolean isTmp;
    private final Path path;
    private final boolean mustBeNew;
    private final Configuration hadoopConf;
    private final FileSystem fileSystem;
    private SettableBlobMeta settableBlobMeta;

    public HdfsBlobStoreFile(Path base, String name, Configuration hconf) {
        if (BLOBSTORE_DATA_FILE.equals(name)) {
            isTmp = false;
        } else {
            Matcher m = TMP_NAME_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new IllegalArgumentException("File name does not match '" + name + "' !~ " + TMP_NAME_PATTERN);
            }
            isTmp = true;
        }
        hadoopConf = hconf;
        key = base.getName();
        path = new Path(base, name);
        mustBeNew = false;
        try {
            fileSystem = path.getFileSystem(hadoopConf);
        } catch (IOException e) {
            throw new RuntimeException("Error getting filesystem for path: " + path, e);
        }
    }

    public HdfsBlobStoreFile(Path base, boolean isTmp, boolean mustBeNew, Configuration hconf) {
        key = base.getName();
        hadoopConf = hconf;
        this.isTmp = isTmp;
        this.mustBeNew = mustBeNew;
        if (this.isTmp) {
            path = new Path(base, System.currentTimeMillis() + TMP_EXT);
        } else {
            path = new Path(base, BLOBSTORE_DATA_FILE);
        }
        try {
            fileSystem = path.getFileSystem(hadoopConf);
        } catch (IOException e) {
            throw new RuntimeException("Error getting filesystem for path: " + path, e);
        }
    }

    @Override
    public void delete() throws IOException {
        fileSystem.delete(path, true);
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
        return fileSystem.getFileStatus(path).getModificationTime();
    }

    private void checkIsNotTmp() {
        if (!isTmp()) {
            throw new IllegalStateException("Can only operate on a temporary blobstore file.");
        }
    }

    private void checkIsTmp() {
        if (isTmp()) {
            throw new IllegalStateException("Cannot operate on a temporary blobstore file.");
        }
    }

    @Override
    public InputStream getInputStream() throws IOException {
        checkIsTmp();
        return fileSystem.open(path);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        checkIsNotTmp();
        OutputStream out = null;
        FsPermission fileperms = new FsPermission(BLOBSTORE_FILE_PERMISSION);
        try {
            out = fileSystem.create(path, (short) this.getMetadata().get_replication_factor());
            fileSystem.setPermission(path, fileperms);
            fileSystem.setReplication(path, (short) this.getMetadata().get_replication_factor());
        } catch (IOException e) {
            //Try to create the parent directory, may not work
            FsPermission dirperms = new FsPermission(HdfsBlobStoreImpl.BLOBSTORE_DIR_PERMISSION);
            if (!fileSystem.mkdirs(path.getParent(), dirperms)) {
                LOG.warn("error creating parent dir: " + path.getParent());
            }
            out = fileSystem.create(path, (short) this.getMetadata().get_replication_factor());
            fileSystem.setPermission(path, dirperms);
            fileSystem.setReplication(path, (short) this.getMetadata().get_replication_factor());
        }
        if (out == null) {
            throw new IOException("Error in creating: " + path);
        }
        return out;
    }

    @Override
    public void commit() throws IOException {
        checkIsNotTmp();
        // FileContext supports atomic rename, whereas FileSystem doesn't
        FileContext fc = FileContext.getFileContext(hadoopConf);
        Path dest = new Path(path.getParent(), BLOBSTORE_DATA_FILE);
        if (mustBeNew) {
            fc.rename(path, dest);
        } else {
            fc.rename(path, dest, Options.Rename.OVERWRITE);
        }
        // Note, we could add support for setting the replication factor
    }

    @Override
    public void cancel() throws IOException {
        checkIsNotTmp();
        delete();
    }

    @Override
    public String toString() {
        return path + ":" + (isTmp ? "tmp" : BlobStoreFile.BLOBSTORE_DATA_FILE) + ":" + key;
    }

    @Override
    public long getFileLength() throws IOException {
        return fileSystem.getFileStatus(path).getLen();
    }

    @Override
    public SettableBlobMeta getMetadata() {
        return settableBlobMeta;
    }

    @Override
    public void setMetadata(SettableBlobMeta meta) {
        this.settableBlobMeta = meta;
    }
}
