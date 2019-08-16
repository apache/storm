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

package org.apache.storm.trident.testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.ITupleCollection;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.snapshot.Snapshottable;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Values;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class LRUMemoryMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T> {

    static ConcurrentHashMap<String, Map<List<Object>, Object>> dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();
    LRUMemoryMapStateBacking<OpaqueValue> backing;
    SnapshottableMap<T> delegate;

    public LRUMemoryMapState(int cacheSize, String id) {
        backing = new LRUMemoryMapStateBacking(cacheSize, id);
        delegate = new SnapshottableMap(OpaqueMap.build(backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    }

    @Override
    public T update(ValueUpdater updater) {
        return delegate.update(updater);
    }

    @Override
    public void set(T o) {
        delegate.set(o);
    }

    @Override
    public T get() {
        return delegate.get();
    }

    @Override
    public void beginCommit(Long txid) {
        delegate.beginCommit(txid);
    }

    @Override
    public void commit(Long txid) {
        delegate.commit(txid);
    }

    @Override
    public Iterator<List<Object>> getTuples() {
        return backing.getTuples();
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        delegate.multiPut(keys, vals);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return delegate.multiGet(keys);
    }

    public static class Factory implements StateFactory {

        String id;
        int maxSize;

        public Factory(int maxSize) {
            id = UUID.randomUUID().toString();
            this.maxSize = maxSize;
        }

        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new LRUMemoryMapState(maxSize, id + partitionIndex);
        }
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    static class LRUMemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {

        Map<List<Object>, T> db;
        Long currTx;

        LRUMemoryMapStateBacking(int cacheSize, String id) {
            if (!dbs.containsKey(id)) {
                dbs.put(id, new LRUMap<List<Object>, Object>(cacheSize));
            }
            this.db = (Map<List<Object>, T>) dbs.get(id);
        }

        public static void clearAll() {
            dbs.clear();
        }

        @Override
        public List<T> multiGet(List<List<Object>> keys) {
            List<T> ret = new ArrayList();
            for (List<Object> key : keys) {
                ret.add(db.get(key));
            }
            return ret;
        }

        @Override
        public void multiPut(List<List<Object>> keys, List<T> vals) {
            for (int i = 0; i < keys.size(); i++) {
                List<Object> key = keys.get(i);
                T val = vals.get(i);
                db.put(key, val);
            }
        }

        @Override
        public Iterator<List<Object>> getTuples() {
            return new Iterator<List<Object>>() {

                private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public List<Object> next() {
                    Map.Entry<List<Object>, T> e = it.next();
                    List<Object> ret = new ArrayList<Object>();
                    ret.addAll(e.getKey());
                    ret.add(((OpaqueValue) e.getValue()).getCurr());
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }
    }
}
