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

package org.apache.storm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.LocalState;
import org.junit.Assert;
import org.junit.Test;

public class LocalStateTest {

    @Test
    public void testLocalState() throws Exception {
        try (TmpPath dir1_tmp = new TmpPath(); TmpPath dir2_tmp = new TmpPath()) {
            GlobalStreamId globalStreamId_a = new GlobalStreamId("a", "a");
            GlobalStreamId globalStreamId_b = new GlobalStreamId("b", "b");
            GlobalStreamId globalStreamId_c = new GlobalStreamId("c", "c");
            GlobalStreamId globalStreamId_d = new GlobalStreamId("d", "d");

            LocalState ls1 = new LocalState(dir1_tmp.getPath(), true);
            LocalState ls2 = new LocalState(dir2_tmp.getPath(), true);
            Assert.assertTrue(ls1.snapshot().isEmpty());

            ls1.put("a", globalStreamId_a);
            ls1.put("b", globalStreamId_b);
            Map<String, GlobalStreamId> expected = new HashMap<>();
            expected.put("a", globalStreamId_a);
            expected.put("b", globalStreamId_b);
            Assert.assertEquals(expected, ls1.snapshot());
            Assert.assertEquals(expected, new LocalState(dir1_tmp.getPath(), true).snapshot());

            Assert.assertTrue(ls2.snapshot().isEmpty());
            ls2.put("b", globalStreamId_a);
            ls2.put("b", globalStreamId_b);
            ls2.put("b", globalStreamId_c);
            ls2.put("b", globalStreamId_d);
            Assert.assertEquals(globalStreamId_d, ls2.get("b"));
        }
    }

    @Test
    public void testEmptyState() throws IOException {
        try (TmpPath tmp_dir = new TmpPath()) {
            GlobalStreamId globalStreamId_a = new GlobalStreamId("a", "a");

            String dir = tmp_dir.getPath();
            LocalState ls = new LocalState(dir, true);

            FileUtils.touch(new File(dir, "12345"));
            FileUtils.touch(new File(dir, "12345.version"));

            Assert.assertNull(ls.get("c"));
            ls.put("a", globalStreamId_a);
            Assert.assertEquals(globalStreamId_a, ls.get("a"));
        }
    }

    @Test
    public void testAllNulState() throws IOException {
        try (TmpPath tmp_dir = new TmpPath()) {
            GlobalStreamId globalStreamId_a = new GlobalStreamId("a", "a");

            String dir = tmp_dir.getPath();
            LocalState ls = new LocalState(dir, true);

            FileUtils.touch(new File(dir, "12345.version"));

            try (FileOutputStream data = FileUtils.openOutputStream(new File(dir, "12345"))) {
                Assert.assertNull(ls.get("c"));
                data.write(new byte[100]);
                ls.put("a", globalStreamId_a);
                Assert.assertEquals(globalStreamId_a, ls.get("a"));
            }
        }
    }
}
