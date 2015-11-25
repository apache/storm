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
package com.alibaba.jstorm.common.metric.old;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import com.alibaba.jstorm.common.metric.old.operator.convert.Convertor;
import com.alibaba.jstorm.common.metric.old.operator.merger.Merger;
import com.alibaba.jstorm.common.metric.old.operator.updater.Updater;
import com.alibaba.jstorm.common.metric.old.window.Metric;

public class Top<T> extends Metric<List<T>, TreeSet<T>> {
    private static final long serialVersionUID = 4990212679365713831L;

    final protected Comparator<T> comparator;
    final protected int n;

    public Top(Comparator<T> comparator, int n) {
        this.comparator = comparator;
        this.n = n;

        this.defaultValue = new TreeSet<T>(comparator);
        this.updater = new TopUpdator<T>(comparator, n);
        this.merger = new TopMerger<T>(comparator, n);
        this.convertor = new SetToList<T>();

        init();
    }

    public static class TopUpdator<T> implements Updater<TreeSet<T>> {
        private static final long serialVersionUID = -3940041101182079146L;

        final protected Comparator<T> comparator;
        final protected int n;

        public TopUpdator(Comparator<T> comparator, int n) {
            this.comparator = comparator;
            this.n = n;
        }

        @SuppressWarnings("unchecked")
        @Override
        public TreeSet<T> update(Number object, TreeSet<T> cache, Object... others) {
            // TODO Auto-generated method stub
            if (cache == null) {
                cache = new TreeSet<T>(comparator);
            }

            cache.add((T) object);

            if (cache.size() > n) {
                cache.remove(cache.last());
            }

            return cache;
        }

        @Override
        public TreeSet<T> updateBatch(TreeSet<T> object, TreeSet<T> cache, Object... objects) {
            // TODO Auto-generated method stub
            if (cache == null) {
                cache = new TreeSet<T>(comparator);
            }

            cache.addAll(object);

            while (cache.size() > n) {
                cache.remove(cache.last());
            }

            return cache;
        }

    }

    public static class TopMerger<T> implements Merger<TreeSet<T>> {

        private static final long serialVersionUID = 4478867986986581638L;
        final protected Comparator<T> comparator;
        final protected int n;

        public TopMerger(Comparator<T> comparator, int n) {
            this.comparator = comparator;
            this.n = n;
        }

        @Override
        public TreeSet<T> merge(Collection<TreeSet<T>> objs, TreeSet<T> unflushed, Object... others) {
            // TODO Auto-generated method stub
            TreeSet<T> temp = new TreeSet<T>(comparator);
            if (unflushed != null) {
                temp.addAll(unflushed);
            }

            for (TreeSet<T> set : objs) {
                temp.addAll(set);
            }

            if (temp.size() <= n) {
                return temp;
            }

            TreeSet<T> ret = new TreeSet<T>(comparator);
            int i = 0;
            for (T item : temp) {
                if (i < n) {
                    ret.add(item);
                    i++;
                } else {
                    break;
                }
            }
            return ret;
        }

    }

    public static class SetToList<T> implements Convertor<TreeSet<T>, List<T>> {
        private static final long serialVersionUID = 4968816655779625255L;

        @Override
        public List<T> convert(TreeSet<T> set) {
            // TODO Auto-generated method stub
            List<T> ret = new ArrayList<T>();
            if (set != null) {
                for (T item : set) {
                    ret.add(item);
                }
            }
            return ret;
        }

    }

}
