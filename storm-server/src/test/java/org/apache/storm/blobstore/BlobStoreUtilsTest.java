/**
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
package org.apache.storm.blobstore;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.storm.nimbus.NimbusInfo;
import org.junit.Test;

public class BlobStoreUtilsTest {

    private static final String KEY = "key";
    private static final String BLOBSTORE_KEY = "/blobstore/" + KEY;

    @SuppressWarnings("unchecked")
    private Map<String, Object> conf = (Map<String, Object>)mock(Map.class);
    private MockZookeeperClientBuilder zkClientBuilder = new MockZookeeperClientBuilder();
    private BlobStore blobStore = mock(BlobStore.class);
    private NimbusInfo nimbusDetails = mock(NimbusInfo.class);

    /**
     * If nimbusDetails are null, the method returns without any Zookeeper calls.
     */
    @Test
    public void testUpdateKeyForBlobStore_nullNimbusInfo() {
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, null);

        zkClientBuilder.verifyExists(false);
        zkClientBuilder.verifyGetChildren(false);
        verify(nimbusDetails, never()).getHost();
        verify(conf, never()).get(anyString());
    }

    /**
     * If the node doesn't exist, the method returns before attempting to fetch children.
     */
    @Test
    public void testUpdateKeyForBlobStore_missingNode() {
        zkClientBuilder.withExists(BLOBSTORE_KEY, false);
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, nimbusDetails);

        zkClientBuilder.verifyExists(true);
        zkClientBuilder.verifyGetChildren(false);
        verify(nimbusDetails, never()).getHost();
        verify(conf, never()).get(anyString());
    }

    /**
     * If the node has null children, the method will exit before calling downloadUpdatedBlob
     * (the config map is first accessed by downloadUpdatedBlob).
     */
    @Test
    public void testUpdateKeyForBlobStore_nodeWithNullChildren() {
        zkClientBuilder.withExists(BLOBSTORE_KEY, true);
        zkClientBuilder.withGetChildren(BLOBSTORE_KEY, (List<String>)null);
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, nimbusDetails);

        zkClientBuilder.verifyExists(true);
        zkClientBuilder.verifyGetChildren();
        verify(nimbusDetails, never()).getHost();
        verify(conf, never()).get(anyString());
    }

    /**
     * If the node has no children, the method behaves the same as for null children.
     */
    @Test
    public void testUpdateKeyForBlobStore_nodeWithEmptyChildren() {
        zkClientBuilder.withExists(BLOBSTORE_KEY, true);
        zkClientBuilder.withGetChildren(BLOBSTORE_KEY);
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, nimbusDetails);

        zkClientBuilder.verifyExists(true);
        zkClientBuilder.verifyGetChildren();
        verify(nimbusDetails, never()).getHost();
        verify(conf, never()).get(anyString());
    }

    /**
     * If the node has children, their hostnames will be checked and if they match,
     * downloadUpdatedBlob will not be called.
     */
    @Test
    public void testUpdateKeyForBlobStore_hostsMatch() {
        zkClientBuilder.withExists(BLOBSTORE_KEY, true);
        zkClientBuilder.withGetChildren(BLOBSTORE_KEY, "localhost:1111-1");
        when(nimbusDetails.getHost()).thenReturn("localhost");
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, nimbusDetails);

        zkClientBuilder.verifyExists(true);
        zkClientBuilder.verifyGetChildren(2);
        verify(nimbusDetails).getHost();
        verify(conf, never()).get(anyString());
    }

    /**
     * If the node has children, their hostnames will be checked and if they don't match,
     * downloadUpdatedBlob will be called.
     */
    @Test
    public void testUpdateKeyForBlobStore_noMatch() {
        zkClientBuilder.withExists(BLOBSTORE_KEY, true);
        zkClientBuilder.withGetChildren(BLOBSTORE_KEY, "localhost:1111-1");
        when(nimbusDetails.getHost()).thenReturn("no match");
        BlobStoreUtils.updateKeyForBlobStore(conf, blobStore, zkClientBuilder.build(), KEY, nimbusDetails);

        zkClientBuilder.verifyExists(true);
        zkClientBuilder.verifyGetChildren(2);
        verify(nimbusDetails).getHost();
        verify(conf, atLeastOnce()).get(anyString());
    }
}
