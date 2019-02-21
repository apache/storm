/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipFile;
import org.apache.storm.testing.TmpPath;
import org.junit.jupiter.api.Test;

public class ServerUtilsTest {

    @Test
    public void testExtractZipFileDisallowsPathTraversal() throws Exception {
        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path extractionDest = testRoot.resolve("dest");
            Files.createDirectories(extractionDest);

            /**
             * Contains good.txt and ../evil.txt. Evil.txt will path outside the target dir, and should not be extracted.
             */
            try (ZipFile zip = new ZipFile(Paths.get("src/test/resources/evil-path-traversal.jar").toFile())) {
                ServerUtils.extractZipFile(zip, extractionDest.toFile(), null);
            }
            
            assertThat(Files.exists(extractionDest.resolve("good.txt")), is(true));
            assertThat(Files.exists(testRoot.resolve("evil.txt")), is(false));
        }
    }
    
    @Test
    public void testExtractZipFileDisallowsPathTraversalWhenUsingPrefix() throws Exception {
        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path destParent = testRoot.resolve("outer");
            Path extractionDest = destParent.resolve("resources");
            Files.createDirectories(extractionDest);

            /**
             * Contains resources/good.txt and resources/../evil.txt. Evil.txt should not be extracted as it would end
             * up outside the extraction dest.
             */
            try (ZipFile zip = new ZipFile(Paths.get("src/test/resources/evil-path-traversal-resources.jar").toFile())) {
                ServerUtils.extractZipFile(zip, extractionDest.toFile(), "resources");
            }
            
            assertThat(Files.exists(extractionDest.resolve("good.txt")), is(true));
            assertThat(Files.exists(extractionDest.resolve("evil.txt")), is(false));
            assertThat(Files.exists(destParent.resolve("evil.txt")), is(false));
        }
    }
}
