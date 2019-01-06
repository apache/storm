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

package org.apache.storm.testing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.storm.daemon.supervisor.DirectoryDeleteVisitor;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TmpPath implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TmpPath.class);
    private final Path path;

    public TmpPath() {
        this(localTempPath());
    }
    
    public TmpPath(String path) {
        this.path = Paths.get(path);
    }
    
    public static String localTempPath() {
        StringBuilder ret = new StringBuilder().append(System.getProperty("java.io.tmpdir"));
        if (!Utils.isOnWindows()) {
            ret.append("/");
        }
        ret.append(Utils.uuid());
        return ret.toString();
    }

    public String getAbsolutePath() {
        return path.toAbsolutePath().toString();
    }
    
    public Path getPath() {
        return path;
    }

    @Override
    public void close() {
        if (path.toFile().exists()) {
            try {
                Files.walkFileTree(path, new DirectoryDeleteVisitor());
            } catch (Exception e) {
                //on windows, the host process still holds lock on the logfile
                LOG.info(e.getMessage());
            }
        }
    }
}
