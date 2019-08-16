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

package org.apache.storm.trident.state.map;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.ValueUpdater;


public class OpaqueMap<T> implements MapState<T> {
    CachedBatchReadsMap<OpaqueValue> backing;
    Long currTx;

    protected OpaqueMap(IBackingMap<OpaqueValue> backing) {
        this.backing = new CachedBatchReadsMap(backing);
    }

    public static <T> MapState<T> build(IBackingMap<OpaqueValue> backing) {
        return new OpaqueMap<T>(backing);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<CachedBatchReadsMap.RetVal<OpaqueValue>> curr = backing.multiGet(keys);
        List<T> ret = new ArrayList<T>(curr.size());
        for (CachedBatchReadsMap.RetVal<OpaqueValue> retval : curr) {
            OpaqueValue val = retval.val;
            if (val != null) {
                if (retval.cached) {
                    ret.add((T) val.getCurr());
                } else {
                    ret.add((T) val.get(currTx));
                }
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<CachedBatchReadsMap.RetVal<OpaqueValue>> curr = backing.multiGet(keys);
        List<OpaqueValue> newVals = new ArrayList<OpaqueValue>(curr.size());
        List<T> ret = new ArrayList<T>();
        for (int i = 0; i < curr.size(); i++) {
            CachedBatchReadsMap.RetVal<OpaqueValue> retval = curr.get(i);
            OpaqueValue<T> val = retval.val;
            ValueUpdater<T> updater = updaters.get(i);
            T prev;
            if (val == null) {
                prev = null;
            } else {
                if (retval.cached) {
                    prev = val.getCurr();
                } else {
                    prev = val.get(currTx);
                }
            }
            T newVal = updater.update(prev);
            ret.add(newVal);
            OpaqueValue<T> newOpaqueVal;
            if (val == null) {
                newOpaqueVal = new OpaqueValue<T>(currTx, newVal);
            } else {
                newOpaqueVal = val.update(currTx, newVal);
            }
            newVals.add(newOpaqueVal);
        }
        backing.multiPut(keys, newVals);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>(vals.size());
        for (T val : vals) {
            updaters.add(new ReplaceUpdater<T>(val));
        }
        multiUpdate(keys, updaters);
    }

    @Override
    public void beginCommit(Long txid) {
        currTx = txid;
        backing.reset();
    }

    @Override
    public void commit(Long txid) {
        currTx = null;
        backing.reset();
    }

    static class ReplaceUpdater<T> implements ValueUpdater<T> {
        T value;

        ReplaceUpdater(T t) {
            value = t;
        }

        @Override
        public T update(Object stored) {
            return value;
        }
    }
}
