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

package org.apache.storm.daemon.logviewer;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogviewerTest {
    private Path mkMockPath(File file) {
        Path mockPath = mock(Path.class);
        when(mockPath.toFile()).thenReturn(file);
        return mockPath;
    }

    private DirectoryStream<Path> mkDirectoryStream(List<Path> listOfPaths) {
        return new DirectoryStream<Path>() {
            @Override
            public Iterator<Path> iterator() {
                return listOfPaths.iterator();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

}
