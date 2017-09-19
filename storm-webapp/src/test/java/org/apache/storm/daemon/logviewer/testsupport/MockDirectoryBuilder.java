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

package org.apache.storm.daemon.logviewer.testsupport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

public class MockDirectoryBuilder {
    private String dirName;
    private long mtime;
    private File[] files;

    /**
     * Constructor.
     */
    public MockDirectoryBuilder() {
        this.dirName = "adir";
        this.mtime = 1;
        this.files = new File[]{};
    }

    public MockDirectoryBuilder setDirName(String name) {
        this.dirName = name;
        return this;
    }

    public MockDirectoryBuilder setMtime(long mtime) {
        this.mtime = mtime;
        return this;
    }

    public MockDirectoryBuilder setFiles(File[] files) {
        this.files = files;
        return this;
    }

    /**
     * Build mocked File object with given (or default) attributes for a directory.
     */
    public File build() {
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn(dirName);
        when(mockFile.lastModified()).thenReturn(mtime);
        when(mockFile.isFile()).thenReturn(false);
        when(mockFile.listFiles()).thenReturn(files);
        try {
            when(mockFile.getCanonicalPath()).thenReturn(dirName);
        } catch (IOException e) {
            // we're making mock, ignoring...
        }
        return mockFile;
    }
}
