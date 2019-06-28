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

package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListDelegate implements List<Object> {
    private List<Object> delegate;

    public ListDelegate() {
        delegate = new ArrayList<>();
    }

    public List<Object> getDelegate() {
        return delegate;
    }

    public void setDelegate(List<Object> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public Iterator<Object> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return delegate.toArray(ts);
    }

    @Override
    public boolean add(Object e) {
        return delegate.add(e);
    }

    @Override
    public void add(int i, Object e) {
        delegate.add(i, e);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public Object remove(int i) {
        return delegate.remove(i);
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        return delegate.containsAll(clctn);
    }

    @Override
    public boolean addAll(Collection<?> clctn) {
        return delegate.addAll(clctn);
    }

    @Override
    public boolean addAll(int i, Collection<?> clctn) {
        return delegate.addAll(i, clctn);
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        return delegate.removeAll(clctn);
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        return delegate.retainAll(clctn);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Object get(int i) {
        return delegate.get(i);
    }

    @Override
    public Object set(int i, Object e) {
        return delegate.set(i, e);
    }

    @Override
    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    @Override
    public ListIterator<Object> listIterator() {
        return delegate.listIterator();
    }

    @Override
    public ListIterator<Object> listIterator(int i) {
        return delegate.listIterator(i);
    }

    @Override
    public List<Object> subList(int i, int i1) {
        return delegate.subList(i, i1);
    }

}
