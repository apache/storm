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

package org.apache.storm.localizer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.daemon.supervisor.IAdvancedFSOps;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.metric.StormMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class LocallyCachedBlobTest {
    private static ClientBlobStore blobStore = Mockito.mock(ClientBlobStore.class);
    private static PortAndAssignment pna = new PortAndAssignmentImpl(6077, new LocalAssignment());
    private static Map<String, Object> conf = new HashMap<>();

    @Test
    public void testNotUsed() throws KeyNotFoundException, AuthorizationException {
        LocallyCachedBlob blob = new LocalizedResource("key", Paths.get("/bogus"), false,
                AdvancedFSOps.make(conf), conf, "user1", new StormMetricsRegistry());
        Assert.assertFalse(blob.isUsed());
        Assert.assertFalse(blob.requiresUpdate(blobStore, -1L));
    }

    @Test
    public void testNotDownloaded() throws KeyNotFoundException, AuthorizationException {
        LocallyCachedBlob blob = new LocalizedResource("key", Paths.get("/bogus"), false,
                AdvancedFSOps.make(conf), conf, "user1", new StormMetricsRegistry());
        blob.addReference(pna, null);
        Assert.assertTrue(blob.isUsed());
        Assert.assertFalse(blob.isFullyDownloaded());
        Assert.assertTrue(blob.requiresUpdate(blobStore, -1L));
    }

    @Test
    public void testOutOfDate() throws KeyNotFoundException, AuthorizationException {
        TestableBlob blob = new TestableBlob("key", Paths.get("/bogus"), false,
                AdvancedFSOps.make(conf), conf, "user1", new StormMetricsRegistry());
        blob.addReference(pna, null);
        Assert.assertTrue(blob.isUsed());
        Assert.assertTrue(blob.isFullyDownloaded());

        // validate blob needs update due to version mismatch
        Assert.assertTrue(blob.requiresUpdate(blobStore, -1L));

        // when blob update time matches remote blobstore update time, validate blob
        // will skip looking at remote version and assume it's up to date
        blob.localUpdateTime = 101L;
        Assert.assertFalse(blob.requiresUpdate(blobStore, 101L));

        // now when the update time on the remote blobstore differs, we should again see that the
        // blob version differs from the remote blobstore
        Assert.assertTrue(blob.requiresUpdate(blobStore, 102L));

        // now validate we don't need any update as versions match, regardless of remote blobstore update time
        blob.localVersion = blob.getRemoteVersion(blobStore);
        Assert.assertFalse(blob.requiresUpdate(blobStore, -1L));
        Assert.assertFalse(blob.requiresUpdate(blobStore, 101L));
        Assert.assertFalse(blob.requiresUpdate(blobStore, 102L));
    }

    public class TestableBlob extends LocalizedResource {
        long localVersion = 9L;

        TestableBlob(String key, Path localBaseDir, boolean shouldUncompress, IAdvancedFSOps fsOps, Map<String, Object> conf, String user, StormMetricsRegistry metricRegistry) {
            super(key, localBaseDir, shouldUncompress, fsOps, conf, user, metricRegistry);
        }

        @Override
        public boolean isFullyDownloaded() {
            return true;
        }

        @Override
        public long getRemoteVersion(ClientBlobStore store) throws KeyNotFoundException, AuthorizationException {
            return 10L;
        }

        @Override
        public long getLocalVersion() {
            return localVersion;
        }
    }
}
