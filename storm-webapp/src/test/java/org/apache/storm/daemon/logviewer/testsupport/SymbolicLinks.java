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

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Assumptions;

/**
 * Create symbolic links in tests. The test is skipped where symbolic links can not be created, e.g. on Windows without the
 * privilege to create them.
 */
public final class SymbolicLinks {

    private SymbolicLinks() {
    }

    /**
     * Create a symbolic link, or skip the test if symbolic links can not be created.
     *
     * @param link the link to create
     * @param target the target of the link
     * @return the link
     */
    public static Path create(Path link, Path target) throws IOException {
        try {
            return Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException e) {
            return Assumptions.abort("Symbolic links are not supported: " + e);
        } catch (FileSystemException e) {
            if (SystemUtils.IS_OS_WINDOWS) {
                return Assumptions.abort("Symbolic links can not be created: " + e);
            }
            throw e;
        }
    }
}
