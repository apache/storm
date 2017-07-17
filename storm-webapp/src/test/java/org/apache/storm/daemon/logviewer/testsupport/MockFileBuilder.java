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

public class MockFileBuilder {
    private String fileName;
    private long mtime;
    private long length;

    /**
     * Constructor.
     */
    public MockFileBuilder() {
        this.fileName = "afile";
        this.mtime = 1;
        this.length = 10 * 1024 * 1024 * 1024;
    }

    public MockFileBuilder setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public MockFileBuilder setMtime(long mtime) {
        this.mtime = mtime;
        return this;
    }

    public MockFileBuilder setLength(long length) {
        this.length = length;
        return this;
    }

    /**
     * Build mocked File object with given (or default) attributes for a file.
     */
    public File build() {
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn(fileName);
        when(mockFile.lastModified()).thenReturn(mtime);
        when(mockFile.isFile()).thenReturn(true);
        try {
            when(mockFile.getCanonicalPath()).thenReturn("/mock/canonical/path/to/" + fileName);
        } catch (IOException e) {
            // we're making mock, ignoring...
        }
        when(mockFile.length()).thenReturn(length);
        return mockFile;
    }
}
