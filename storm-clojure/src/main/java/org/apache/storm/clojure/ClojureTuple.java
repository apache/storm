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

import clojure.lang.AFn;
import clojure.lang.ASeq;
import clojure.lang.ArityException;
import clojure.lang.Counted;
import clojure.lang.IFn;
import clojure.lang.ILookup;
import clojure.lang.IMapEntry;
import clojure.lang.IMeta;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import clojure.lang.Keyword;
import clojure.lang.MapEntry;
import clojure.lang.Obj;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Seqable;
import clojure.lang.Symbol;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

public class ClojureTuple extends TupleImpl implements Seqable, Indexed, IMeta, ILookup, IPersistentMap, Map, IFn {
    private IPersistentMap meta;
    private IPersistentMap map;

    public ClojureTuple(Tuple t) {
        super(t);
    }

    private Keyword makeKeyword(String name) {
        return Keyword.intern(Symbol.create(name));
    }

    private PersistentArrayMap toMap() {
        Object[] array = new Object[size() * 2];
        List<String> fields = getFields().toList();
        for (int i = 0; i < size(); i++) {
            array[i * 2] = fields.get(i);
            array[(i * 2) + 1] = getValue(i);
        }
        return new PersistentArrayMap(array);
    }

    public IPersistentMap getMap() {
        if (map == null) {
            map = toMap();
        }
        return map;
    }

    /* ILookup */
    @Override
    public Object valAt(Object o) {
        try {
            if (o instanceof Keyword) {
                return getValueByField(((Keyword) o).getName());
            } else if (o instanceof String) {
                return getValueByField((String) o);
            }
        } catch (IllegalArgumentException ignored) {
            //ignore
        }
        return null;
    }

    @Override
    public Object valAt(Object o, Object def) {
        Object ret = valAt(o);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    /* Seqable */
    @Override
    public ISeq seq() {
        if (size() > 0) {
            return new Seq(getFields().toList(), getValues(), 0);
        }
        return null;
    }

    static class Seq extends ASeq implements Counted {
        private static final long serialVersionUID = 1L;
        final List<String> fields;
        final List<Object> values;
        final int count;

        Seq(List<String> fields, List<Object> values, int count) {
            this.fields = fields;
            this.values = values;
            assert count >= 0;
            this.count = count;
        }

        Seq(IPersistentMap meta, List<String> fields, List<Object> values, int count) {
            super(meta);
            this.fields = fields;
            this.values = values;
            assert count >= 0;
            this.count = count;
        }

        @Override
        public Object first() {
            return new MapEntry(fields.get(count), values.get(count));
        }

        @Override
        public ISeq next() {
            if (count + 1 < fields.size()) {
                return new Seq(fields, values, count + 1);
            }
            return null;
        }

        @Override
        public int count() {
            assert fields.size() - count >= 0 : "index out of bounds";
            // i being the position in the fields of this seq, the remainder of the seq is the size
            return fields.size() - count;
        }

        @Override
        public Obj withMeta(IPersistentMap meta) {
            return new Seq(meta, fields, values, count);
        }
    }

    /* Indexed */
    @Override
    public Object nth(int i) {
        if (i < size()) {
            return getValue(i);
        } else {
            return null;
        }
    }

    @Override
    public Object nth(int i, Object notfound) {
        Object ret = nth(i);
        if (ret == null) {
            ret = notfound;
        }
        return ret;
    }

    /* Counted */
    @Override
    public int count() {
        return size();
    }

    /* IMeta */
    @Override
    public IPersistentMap meta() {
        if (meta == null) {
            meta = new PersistentArrayMap(new Object[] {
                    makeKeyword("stream"), getSourceStreamId(),
                    makeKeyword("component"), getSourceComponent(),
                    makeKeyword("task"), getSourceTask()});
        }
        return meta;
    }

    /* IFn */
    @Override
    public Object invoke(Object o) {
        return valAt(o);
    }

    @Override
    public Object invoke(Object o, Object notfound) {
        return valAt(o, notfound);
    }

    @Override
    public Object invoke() {
        throw new ArityException(0, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3) {
        throw new ArityException(3, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4) {
        throw new ArityException(4, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        throw new ArityException(5, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        throw new ArityException(6, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7) {
        throw new ArityException(7, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8) {
        throw new ArityException(8, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9) {
        throw new ArityException(9, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10) {
        throw new ArityException(10, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11) {
        throw new ArityException(11, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12) {
        throw new ArityException(12, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13) {
        throw new ArityException(13, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14) {
        throw new ArityException(14, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15) {
        throw new ArityException(15, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16) {
        throw new ArityException(16, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16, Object arg17) {
        throw new ArityException(17, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16, Object arg17, Object arg18) {
        throw new ArityException(18, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16, Object arg17, Object arg18, Object arg19) {
        throw new ArityException(19, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16, Object arg17, Object arg18, Object arg19, Object arg20) {
        throw new ArityException(20, "1 or 2 args only");
    }

    @Override
    public Object invoke(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7,
            Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
            Object arg15, Object arg16, Object arg17, Object arg18, Object arg19, Object arg20, Object... args) {
        throw new ArityException(21, "1 or 2 args only");
    }

    @Override
    public Object applyTo(ISeq arglist) {
        return AFn.applyToHelper(this, arglist);
    }

    @Override
    public Object call() throws Exception {
        return invoke();
    }

    @Override
    public void run() {
        invoke();
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
