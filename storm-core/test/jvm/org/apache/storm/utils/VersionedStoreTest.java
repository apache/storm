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

package org.apache.storm.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.storm.testing.TmpPath;
import org.junit.jupiter.api.Test;

/**
 * Ported from versioned_store_test.clj.
 */
class VersionedStoreTest {

    private void writeToFile(String file) throws IOException {
        FileUtils.writeStringToFile(new File(file), "time:" + System.currentTimeMillis(), "UTF-8");
    }

    @Test
    void testEmptyVersion() throws Exception {
        try (TmpPath dir = new TmpPath()) {
            VersionedStore vs = new VersionedStore(dir.getPath(), true);
            String v = vs.createVersion();
            vs.succeedVersion(v);
            writeToFile(v);
            assertEquals(1, vs.getAllVersions().size());
            assertEquals(v, vs.mostRecentVersionPath());
        }
    }

    @Test
    void testMultipleVersions() throws Exception {
        try (TmpPath dir = new TmpPath()) {
            VersionedStore vs = new VersionedStore(dir.getPath(), true);

            String v1 = vs.createVersion();
            vs.succeedVersion(v1);
            writeToFile(v1);

            Thread.sleep(100);

            String v2 = vs.createVersion();
            vs.succeedVersion(v2);
            writeToFile(v2);

            assertEquals(2, vs.getAllVersions().size());
            assertEquals(v2, vs.mostRecentVersionPath());

            // Creating a version without succeeding it should not change the most recent
            vs.createVersion();
            assertEquals(v2, vs.mostRecentVersionPath());
        }
    }
}
