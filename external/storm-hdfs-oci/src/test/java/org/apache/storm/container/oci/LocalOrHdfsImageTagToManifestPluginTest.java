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

package org.apache.storm.container.oci;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LocalOrHdfsImageTagToManifestPluginTest {

    private static final String KNOWN_HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String UNKNOWN_HASH = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    //same length as a hash, but made of characters that would still escape the manifest directory
    private static final String HASH_LENGTH_PATH = "../../../user/foo/bar/" + "a".repeat(42);

    @TempDir
    Path tempDir;

    private LocalOrHdfsImageTagToManifestPlugin createPlugin() throws IOException {
        Path hashFile = tempDir.resolve("image-tag-to-hash");
        Files.write(hashFile, ("busybox:latest:" + KNOWN_HASH + "\n").getBytes(StandardCharsets.UTF_8));

        Map<String, Object> conf = new HashMap<>();
        conf.put("storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.local.hash.file", hashFile.toString());
        conf.put(DaemonConfig.STORM_OCI_IMAGE_HDFS_TOPLEVEL_DIR, "/storm/oci");

        LocalOrHdfsImageTagToManifestPlugin plugin = new LocalOrHdfsImageTagToManifestPlugin();
        plugin.init(conf);
        return plugin;
    }

    @Test
    public void testKnownImageTagIsMappedToItsHash() throws Exception {
        assertEquals(KNOWN_HASH, createPlugin().getHashFromImageTag("busybox:latest"));
    }

    @Test
    public void testUnmappedImageTagIsUsedAsHashWhenItLooksLikeOne() throws Exception {
        assertEquals(UNKNOWN_HASH, createPlugin().getHashFromImageTag(UNKNOWN_HASH));
    }

    @Test
    public void testUnmappedImageTagThatIsNotAHashIsRejected() throws Exception {
        LocalOrHdfsImageTagToManifestPlugin plugin = createPlugin();
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag("../../../user/foo/bar"));
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag("busybox:unknown"));
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag(".."));
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag("/etc/passwd"));
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag(HASH_LENGTH_PATH));
        assertThrows(UncheckedIOException.class, () -> plugin.getHashFromImageTag(UNKNOWN_HASH + "a"));
    }

    @Test
    public void testGetManifestFromImageTagRejectsUnmappedNonHashTag() throws Exception {
        LocalOrHdfsImageTagToManifestPlugin plugin = createPlugin();
        assertThrows(UncheckedIOException.class, () -> plugin.getManifestFromImageTag("../../../user/foo/bar"));
        assertThrows(UncheckedIOException.class, () -> plugin.getManifestFromImageTag(HASH_LENGTH_PATH));
    }
}
