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


import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.supervisor.IAdvancedFSOps;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.EquivalenceUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.storm.metric.StormMetricsRegistry;

public class LocalizedResourceRetentionSetTest {

    @Test
    public void testAddResources() throws Exception {
        PortAndAssignment pna1 = new PortAndAssignmentImpl(1, new LocalAssignment("topo1", Collections.emptyList()));
        PortAndAssignment pna2 = new PortAndAssignmentImpl(1, new LocalAssignment("topo2", Collections.emptyList()));
        String user = "user";
        Map<String, Object> conf = new HashMap<>();
        IAdvancedFSOps ops = mock(IAdvancedFSOps.class);
        LocalizedResourceRetentionSet lrretset = new LocalizedResourceRetentionSet(10);
        ConcurrentMap<String, LocalizedResource> lrset = new ConcurrentHashMap<>();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        LocalizedResource localresource1 = new LocalizedResource("key1", Paths.get("testfile1"), false, ops, conf, user, metricsRegistry);
        localresource1.addReference(pna1, null);
        LocalizedResource localresource2 = new LocalizedResource("key2", Paths.get("testfile2"), false, ops, conf, user, metricsRegistry);
        localresource2.addReference(pna1, null);
        // check adding reference to local resource with topology of same name
        localresource2.addReference(pna2, null);

        lrset.put("key1", localresource1);
        lrset.put("key2", localresource2);
        lrretset.addResources(lrset);
        assertEquals("number to clean is not 0 " + lrretset.noReferences, 0, lrretset.getSizeWithNoReferences());
        assertTrue(localresource1.removeReference(pna1));
        lrretset = new LocalizedResourceRetentionSet(10);
        lrretset.addResources(lrset);
        assertEquals("number to clean is not 1 " + lrretset.noReferences, 1, lrretset.getSizeWithNoReferences());

        assertTrue(localresource2.removeReference(pna1));
        lrretset = new LocalizedResourceRetentionSet(10);
        lrretset.addResources(lrset);
        assertEquals("number to clean is not 1  " + lrretset.noReferences, 1, lrretset.getSizeWithNoReferences());

        assertTrue(localresource2.removeReference(pna2));
        lrretset = new LocalizedResourceRetentionSet(10);
        lrretset.addResources(lrset);
        assertEquals("number to clean is not 2 " + lrretset.noReferences, 2, lrretset.getSizeWithNoReferences());
    }

    @Test
    public void testRemoveEquivalent() {
        ExecutorInfo a = new ExecutorInfo(1, 1);
        ExecutorInfo b = new ExecutorInfo(2, 2);
        ExecutorInfo c = new ExecutorInfo(3, 3);

        List<ExecutorInfo> origList = new ArrayList<>();
        origList.add(a);
        origList.add(b);

        List<ExecutorInfo> equivList = new ArrayList<>();
        equivList.add(b);
        equivList.add(a);

        List<ExecutorInfo> differentList = new ArrayList<>();
        differentList.add(a);
        differentList.add(c);

        LocalAssignment laOrig = new LocalAssignment("topo1", origList);
        LocalAssignment laEquiv = new LocalAssignment("topo1", equivList);
        LocalAssignment laDifferent = new LocalAssignment("topo1", differentList);

        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(laOrig, laEquiv));
        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(laOrig, laDifferent));

        PortAndAssignment pnaOrig = new PortAndAssignmentImpl(1, laOrig);
        PortAndAssignment pnaEquiv = new PortAndAssignmentImpl(1, laEquiv);
        PortAndAssignment pnaDifferent = new PortAndAssignmentImpl(1, laDifferent);

        assertTrue(pnaOrig.isEquivalentTo(pnaEquiv));
        assertFalse(pnaOrig.isEquivalentTo(pnaDifferent));

        LocalizedResource localresource = new LocalizedResource("key1", Paths.get("testfile1"),
                false, mock(IAdvancedFSOps.class), new HashMap<>(), "user", new StormMetricsRegistry());
        localresource.addReference(pnaOrig, null);

        assertFalse(localresource.removeReference(pnaDifferent));
        assertTrue(localresource.removeReference(pnaEquiv));
    }

    @Test
    public void testCleanup() throws Exception {
        ClientBlobStore mockBlobstore = mock(ClientBlobStore.class);
        when(mockBlobstore.getBlobMeta(any())).thenReturn(new ReadableBlobMeta(new SettableBlobMeta(), 1));
        PortAndAssignment pna1 = new PortAndAssignmentImpl(1, new LocalAssignment("topo1", Collections.emptyList()));
        String user = "user";
        Map<String, Object> conf = new HashMap<>();
        IAdvancedFSOps ops = mock(IAdvancedFSOps.class);
        LocalizedResourceRetentionSet lrretset = spy(new LocalizedResourceRetentionSet(10));
        ConcurrentMap<String, LocalizedResource> lrFiles = new ConcurrentHashMap<>();
        ConcurrentMap<String, LocalizedResource> lrArchives = new ConcurrentHashMap<>();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        // no reference to key1
        LocalizedResource localresource1 = new LocalizedResource("key1", Paths.get("./target/TESTING/testfile1"), false, ops, conf,
                                                                 user, metricsRegistry);
        localresource1.setSize(10);
        // no reference to archive1
        LocalizedResource archiveresource1 = new LocalizedResource("archive1", Paths.get("./target/TESTING/testarchive1"), true, ops,
                                                                   conf, user, metricsRegistry);
        archiveresource1.setSize(20);
        // reference to key2
        LocalizedResource localresource2 = new LocalizedResource("key2", Paths.get("./target/TESTING/testfile2"), false, ops, conf,
                                                                 user, metricsRegistry);
        localresource2.addReference(pna1, null);
        // check adding reference to local resource with topology of same name
        localresource2.addReference(pna1, null);
        localresource2.setSize(10);
        lrFiles.put("key1", localresource1);
        lrFiles.put("key2", localresource2);
        lrArchives.put("archive1", archiveresource1);

        lrretset.addResources(lrFiles);
        lrretset.addResources(lrArchives);
        assertEquals("number to clean is not 2", 2, lrretset.getSizeWithNoReferences());

        lrretset.cleanup(mockBlobstore);
        assertEquals("resource not cleaned up", 0, lrretset.getSizeWithNoReferences());
    }
}
