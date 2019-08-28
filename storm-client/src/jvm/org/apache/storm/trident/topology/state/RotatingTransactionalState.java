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

package org.apache.storm.trident.topology.state;

import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RotatingTransactionalState {
    private static final Logger LOG = LoggerFactory.getLogger(RotatingTransactionalState.class);
    private TransactionalState state;
    private String subdir;
    private TreeMap<Long, Object> curr = new TreeMap<Long, Object>();

    public RotatingTransactionalState(TransactionalState state, String subdir) {
        this.state = state;
        this.subdir = subdir;
        state.mkdir(subdir);
        sync();
        LOG.debug("Created {}", this);
    }

    public Object getLastState() {
        if (curr.isEmpty()) {
            return null;
        } else {
            return curr.lastEntry().getValue();
        }
    }

    public void overrideState(long txid, Object state) {
        LOG.debug("Overriding state. [txid = {}],  [state = {}]", txid, state);
        LOG.trace("[{}]", this);

        this.state.setData(txPath(txid), state);
        curr.put(txid, state);

        LOG.trace("Overriding state complete.  [{}]", this);
    }

    public void removeState(long txid) {
        Object state = null;
        if (curr.containsKey(txid)) {
            state = curr.remove(txid);
            this.state.delete(txPath(txid));
        }
        LOG.debug("Removed [state = {}], [txid = {}]", state, txid);
        LOG.trace("[{}]", this);
    }

    public Object getState(long txid) {
        final Object state = curr.get(txid);
        LOG.debug("Getting state. [txid = {}] => [state = {}]", txid, state);
        LOG.trace("Internal state [{}]", this);
        return state;
    }

    public Object getState(long txid, StateInitializer init) {
        if (!curr.containsKey(txid)) {
            SortedMap<Long, Object> prevMap = curr.headMap(txid);
            SortedMap<Long, Object> afterMap = curr.tailMap(txid);

            Long prev = null;
            if (!prevMap.isEmpty()) {
                prev = prevMap.lastKey();
            }

            Object data;
            if (afterMap.isEmpty()) {
                Object prevData;
                if (prev != null) {
                    prevData = curr.get(prev);
                } else {
                    prevData = null;
                }
                data = init.init(txid, prevData);
            } else {
                data = null;
            }
            curr.put(txid, data);
            state.setData(txPath(txid), data);
        }
        Object state = curr.get(txid);
        LOG.debug("Getting or initializing state. [txid = {}] => [state = {}]", txid, state);
        LOG.trace("[{}]", this);
        return state;
    }

    public Object getPreviousState(long txid) {
        final SortedMap<Long, Object> prevMap = curr.headMap(txid);
        Object state;

        if (prevMap.isEmpty()) {
            state = null;
        } else {
            state = prevMap.get(prevMap.lastKey());
        }

        LOG.debug("Getting previous [state = {}], [txid = {}]", state, txid);
        LOG.trace("[{}]", this);
        return state;
    }

    public boolean hasCache(long txid) {
        return curr.containsKey(txid);
    }

    /**
     * Returns null if it was created, the value otherwise.
     */
    public Object getStateOrCreate(long txid, StateInitializer init) {
        Object state;
        if (curr.containsKey(txid)) {
            state = curr.get(txid);
        } else {
            getState(txid, init);
            state = null;
        }
        return state;
    }

    public void cleanupBefore(long txid) {
        SortedMap<Long, Object> toDelete = curr.headMap(txid);
        for (long tx : new HashSet<Long>(toDelete.keySet())) {
            curr.remove(tx);
            try {
                state.delete(txPath(tx));
            } catch (RuntimeException e) {
                // Ignore NoNodeExists exceptions because when sync() it may populate curr with stale data since
                // zookeeper reads are eventually consistent.
                if (!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    throw e;
                }
            }
        }
    }

    private void sync() {
        List<String> txids = state.list(subdir);
        for (String txid : txids) {
            Object data = state.getData(txPath(txid));
            curr.put(Long.parseLong(txid), data);
        }
    }

    private String txPath(long tx) {
        return txPath("" + tx);
    }

    private String txPath(String tx) {
        return subdir + "/" + tx;
    }

    @Override
    public String toString() {
        return "RotatingTransactionalState{"
                + "state=" + state
                + ", subdir='" + subdir + '\''
                + ", curr=" + curr
                + '}';
    }

    public interface StateInitializer {
        Object init(long txid, Object lastState);
    }
}
