/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.trident;

import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.api.CreateBuilder;
import org.apache.storm.shade.org.apache.curator.framework.api.ProtectACLCreateModeStatPathAndBytesable;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.CombinerValueUpdater;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.trident.testing.MemoryBackingMap;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.topology.state.TestTransactionalState;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StateTest {

    private void singleRemove(MemoryMapState<Object> map, Object key){
        List<List<Object>> keys = Collections.singletonList(Collections.singletonList(key));
        map.multiRemove(keys);
    }

    private void singlePut(MemoryMapState<Object> map, Object key, Object val){
        List<List<Object>> keys = Collections.singletonList(Collections.singletonList(key));
        List<Object> vals = Collections.singletonList(val);
        map.multiPut(keys, vals);
    }

    private Object singleGet(MapState<Object> map, Object key){
        List<List<Object>> keys = Collections.singletonList(Collections.singletonList(key));
        return map.multiGet(keys).get(0);
    }

    private Object singleUpdate(MapState<Object> map, Object key, Long amt){
        List<List<Object>> keys = Collections.singletonList(Collections.singletonList(key));
        CombinerValueUpdater valueUpdater = new CombinerValueUpdater(new Count(), amt);
        List<ValueUpdater> updaters = Collections.singletonList(valueUpdater);
        return map.multiUpdate(keys, updaters).get(0);
    }

    @Test
    public void testOpaqueValue() {
        OpaqueValue<String> opqval = new OpaqueValue<>(8L, "v1", "v0");
        OpaqueValue<String> upval0 = opqval.update(8L, "v2");
        OpaqueValue<String> upval1 = opqval.update(9L, "v2");
        assertEquals(opqval.get(null), "v1");
        assertEquals(opqval.get(100L), "v1");
        assertEquals(opqval.get(9L), "v1");
        assertEquals(opqval.get(8L), "v0");
        Assertions.assertThrows(Exception.class, () -> opqval.get(7L));
        assertEquals(opqval.getPrev(), "v0");
        assertEquals(opqval.getCurr(), "v1");
        // update with current
        assertEquals(upval0.getPrev(), "v0");
        assertEquals(upval0.getCurr(), "v2");
        assertNotEquals(opqval, upval0);
        // update
        assertEquals(upval1.getPrev(), "v1");
        assertEquals(upval1.getCurr(), "v2");
        assertNotEquals(opqval, upval1);
    }

    @Test
    public void testOpaqueMap() {
        MapState<Object> map = OpaqueMap.build(new MemoryBackingMap<>());
        map.beginCommit(1L);
        assertEquals(singleGet(map, "a"), null);
        // tests that intra-batch caching works
        assertEquals(singleUpdate(map, "a", 1L), 1L);
        assertEquals(singleGet(map, "a"), 1L);
        assertEquals(singleUpdate(map, "a", 2L), 3L);
        assertEquals(singleGet(map, "a"), 3L);
        map.commit(1L);
        map.beginCommit(1L);
        assertEquals(singleGet(map, "a"), null);
        assertEquals(singleUpdate(map, "a", 2L), 2L);
        map.commit(1L);
        map.beginCommit(2L);
        assertEquals(singleGet(map, "a"), 2L);
        assertEquals(singleUpdate(map, "a", 3L), 5L);
        assertEquals(singleUpdate(map, "a", 1L), 6L);
        map.commit(2L);
    }

    @Test
    public void testTransactionalMap() {
        MapState<Object> map = TransactionalMap.build(new MemoryBackingMap<>());
        map.beginCommit(1L);
        assertEquals(singleGet(map, "a"), null);
        // tests that intra-batch caching works
        assertEquals(singleUpdate(map, "a", 1L), 1L);
        assertEquals(singleUpdate(map, "a", 2L), 3L);
        map.commit(1L);
        map.beginCommit(1L);
        assertEquals(singleGet(map, "a"), 3L);
        // tests that intra-batch caching has no effect if it's the same commit as previous commit
        assertEquals(singleUpdate(map, "a", 1L), 3L);
        assertEquals(singleUpdate(map, "a", 2L), 3L);
        map.commit(1L);
        map.beginCommit(2L);
        assertEquals(singleGet(map, "a"), 3L);
        assertEquals(singleUpdate(map, "a", 3L), 6L);
        assertEquals(singleUpdate(map, "a", 1L), 7L);
        map.commit(2L);
    }

    @Test
    public void testCreateNodeAcl() throws Exception {
        // Creates ZooKeeper nodes with the correct ACLs
        CuratorFramework curator = Mockito.mock(CuratorFramework.class);
        CreateBuilder builder0 = Mockito.mock(CreateBuilder.class);
        ProtectACLCreateModeStatPathAndBytesable builder1 = Mockito.mock(ProtectACLCreateModeStatPathAndBytesable.class);
        List<ACL> expectedAcls = ZooDefs.Ids.CREATOR_ALL_ACL;
        Mockito.when(curator.create()).thenReturn(builder0);
        Mockito.when(builder0.creatingParentsIfNeeded()).thenReturn(builder1);
        Mockito.when(builder1.withMode(ArgumentMatchers.isA(CreateMode.class))).thenReturn(builder1);
        Mockito.when(builder1.withACL(Mockito.anyList())).thenReturn(builder1);
        TestTransactionalState.createNode(curator, "", new byte[0], expectedAcls, null);
        Mockito.verify(builder1).withACL(expectedAcls);
    }

    @Test
    public void testMemoryMapStateRemove() {
        MemoryMapState<Object> map = new MemoryMapState<>(Utils.uuid());
        map.beginCommit(1L);
        singlePut(map, "a", 1);
        singlePut(map, "b", 2);
        map.commit(1L);
        map.beginCommit(2L);
        singleRemove(map, "a");
        assertEquals(singleGet(map, "a"), null);
        assertEquals(singleGet(map, "b"), 2);
        map.commit(2L);
        map.beginCommit(2L);
        assertEquals(singleGet(map, "a"), 1);
        assertEquals(singleGet(map, "b"), 2);
        singleRemove(map, "a");
        map.commit(2L);
        map.beginCommit(3L);
        assertEquals(singleGet(map, "a"), null);
        assertEquals(singleGet(map, "b"), 2);
        map.commit(3L);
    }
}
