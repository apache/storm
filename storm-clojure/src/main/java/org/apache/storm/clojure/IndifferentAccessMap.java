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

package org.apache.storm.clojure;

import clojure.lang.ILookup;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class IndifferentAccessMap  implements ILookup, IPersistentMap, Map {

    protected IPersistentMap map;

    public IndifferentAccessMap(IPersistentMap map) {
        setMap(map);
    }

    public IPersistentMap getMap() {
        return map;
    }

    public IPersistentMap setMap(IPersistentMap map) {
        this.map = map;
        return this.map;
    }

    @Override
    public int size() {
        return ((Map) getMap()).size();
    }

    @Override
    public int count() {
        return size();
    }

    @Override
    public ISeq seq() {
        return getMap().seq();
    }

    @Override
    public Object valAt(Object o) {
        if (o instanceof Keyword) {
            return valAt(((Keyword) o).getName());
        }
        return getMap().valAt(o);
    }
    
    @Override
    public Object valAt(Object o, Object def) {
        Object ret = valAt(o);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    /* IPersistentMap */
    /* Naive implementation, but it might be good enough */
    @Override
    public IPersistentMap assoc(Object k, Object v) {
        if (k instanceof Keyword) {
            return assoc(((Keyword) k).getName(), v);
        }
        return new IndifferentAccessMap(getMap().assoc(k, v));
    }

    @Override
    public IPersistentMap assocEx(Object k, Object v) {
        if (k instanceof Keyword) {
            return assocEx(((Keyword) k).getName(), v);
        }
        return new IndifferentAccessMap(getMap().assocEx(k, v));
    }

    @Override
    public IPersistentMap without(Object k) {
        if (k instanceof Keyword) {
            return without(((Keyword) k).getName());
        }
        return new IndifferentAccessMap(getMap().without(k));
    }

    @Override
    public boolean containsKey(Object k) {
        if (k instanceof Keyword) {
            return containsKey(((Keyword) k).getName());
        }
        return getMap().containsKey(k);
    }

    @Override
    public IMapEntry entryAt(Object k) {
        if (k instanceof Keyword) {
            return entryAt(((Keyword) k).getName());
        }
        return getMap().entryAt(k);
    }

    @Override
    public IPersistentCollection cons(Object o) {
        return getMap().cons(o);
    }

    @Override
    public IPersistentCollection empty() {
        return new IndifferentAccessMap(PersistentArrayMap.EMPTY);
    }

    @Override
    public boolean equiv(Object o) {
        return getMap().equiv(o);
    }

    @Override
    public Iterator iterator() {
        return getMap().iterator();
    }

    /* Map */
    @Override
    public boolean containsValue(Object v) {
        return ((Map) getMap()).containsValue(v);
    }

    @Override
    public Set entrySet() {
        return ((Map) getMap()).entrySet();
    }

    @Override
    public Object get(Object k) {
        return valAt(k);
    }

    @Override
    public boolean isEmpty() {
        return ((Map) getMap()).isEmpty();
    }

    @Override
    public Set keySet() {
        return ((Map) getMap()).keySet();
    }

    @Override
    public Collection values() {
        return ((Map) getMap()).values();
    }
    
    /* Not implemented */
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object put(Object k, Object v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object k) {
        throw new UnsupportedOperationException();
    }
}
