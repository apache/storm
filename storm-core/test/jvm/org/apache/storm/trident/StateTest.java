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
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

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
        assertThat(opqval.get(null), is("v1"));
        assertThat(opqval.get(100L), is("v1"));
        assertThat(opqval.get(9L), is("v1"));
        assertThat(opqval.get(8L), is("v0"));
        Assertions.assertThrows(Exception.class, () -> opqval.get(7L));
        assertThat(opqval.getPrev(), is("v0"));
        assertThat(opqval.getCurr(), is("v1"));
        // update with current
        assertThat(upval0.getPrev(), is("v0"));
        assertThat(upval0.getCurr(), is("v2"));
        Assertions.assertNotSame(opqval, upval0);
        // update
        assertThat(upval1.getPrev(), is("v1"));
        assertThat(upval1.getCurr(), is("v2"));
        Assertions.assertNotSame(opqval, upval1);
    }

    @Test
    public void testOpaqueMap() {
        MapState<Object> map = OpaqueMap.build(new MemoryBackingMap<>());
        map.beginCommit(1L);
        assertThat(singleGet(map, "a"), nullValue());
        // tests that intra-batch caching works
        assertThat(singleUpdate(map, "a", 1L), is(1L));
        assertThat(singleGet(map, "a"), is(1L));
        assertThat(singleUpdate(map, "a", 2L), is(3L));
        assertThat(singleGet(map, "a"), is(3L));
        map.commit(1L);
        map.beginCommit(1L);
        assertThat(singleGet(map, "a"), nullValue());
        assertThat(singleUpdate(map, "a", 2L), is(2L));
        map.commit(1L);
        map.beginCommit(2L);
        assertThat(singleGet(map, "a"), is(2L));
        assertThat(singleUpdate(map, "a", 3L), is(5L));
        assertThat(singleUpdate(map, "a", 1L), is(6L));
        map.commit(2L);
    }

    @Test
    public void testTransactionalMap() {
        MapState<Object> map = TransactionalMap.build(new MemoryBackingMap<>());
        map.beginCommit(1L);
        assertThat(singleGet(map, "a"), nullValue());
        // tests that intra-batch caching works
        assertThat(singleUpdate(map, "a", 1L), is(1L));
        assertThat(singleUpdate(map, "a", 2L), is(3L));
        map.commit(1L);
        map.beginCommit(1L);
        assertThat(singleGet(map, "a"), is(3L));
        // tests that intra-batch caching has no effect if it's the same commit as previous commit
        assertThat(singleUpdate(map, "a", 1L), is(3L));
        assertThat(singleUpdate(map, "a", 2L), is(3L));
        map.commit(1L);
        map.beginCommit(2L);
        assertThat(singleGet(map, "a"), is(3L));
        assertThat(singleUpdate(map, "a", 3L), is(6L));
        assertThat(singleUpdate(map, "a", 1L), is(7L));
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
        Mockito.when(builder1.withMode(Matchers.isA(CreateMode.class))).thenReturn(builder1);
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
        assertThat(singleGet(map, "a"), nullValue());
        assertThat(singleGet(map, "b"), is(2));
        map.commit(2L);
        map.beginCommit(2L);
        assertThat(singleGet(map, "a"), is(1));
        assertThat(singleGet(map, "b"), is(2));
        singleRemove(map, "a");
        map.commit(2L);
        map.beginCommit(3L);
        assertThat(singleGet(map, "a"), nullValue());
        assertThat(singleGet(map, "b"), is(2));
        map.commit(3L);
    }
}
