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
package org.apache.storm.scheduler.blacklist;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class CircularBuffer<T extends Serializable> extends AbstractCollection<T> implements Serializable {

    // This is the largest capacity allowed by this implementation
    private static final int MAX_CAPACITY = 1 << 30;

    private int size = 0;
    private int producerIndex = 0;
    private int consumerIndex = 0;

    private int capacity;

    private Serializable[] underlying;

    // Construct a buffer which has at least the specified capacity.  If
    // the value specified is a power of two then the buffer will be
    // exactly the specified size.  Otherwise the buffer will be the
    // first power of two which is greater than the specified value.
    public CircularBuffer(int capacity) {

        if (capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("Capacity greater than " +
                    "allowed");
        }

        this.capacity = capacity;
        underlying = new Serializable[this.capacity];
    }

    // Constructor used by clone()
    private CircularBuffer(CircularBuffer oldBuffer) {
        size = oldBuffer.size;
        producerIndex = oldBuffer.producerIndex;
        consumerIndex = oldBuffer.consumerIndex;
        capacity = oldBuffer.capacity;
        //bitmask = oldBuffer.bitmask;
        underlying = new Serializable[oldBuffer.underlying.length];
        System.arraycopy(oldBuffer.underlying, 0, underlying, 0, underlying.length);
    }

    private boolean isFull() {
        return size == capacity;
    }

    public boolean add(Serializable obj) {
        if (isFull()) {
            remove();
        }

        size++;
        underlying[producerIndex] = obj;

        producerIndex = (producerIndex + 1) % capacity;

        return true;
    }

    public T remove() {
        Serializable obj;

        if (size == 0) return null;

        size--;
        obj = underlying[consumerIndex];
        underlying[consumerIndex] = null; // allow gc to collect

        consumerIndex = (consumerIndex + 1) % capacity;

        return (T) obj;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public Serializable peek() {
        if (size == 0) return null;
        return underlying[consumerIndex];
    }

    public void clear() {
        Arrays.fill(underlying, null);
        size = 0;
        producerIndex = 0;
        consumerIndex = 0;
    }

    public Object clone() {
        return new CircularBuffer(this);
    }

    public String toString() {
        StringBuffer s = new StringBuffer(super.toString() + "' size: '" + size() + "'");
        return s.toString();
    }

    public List<T> toList() {
        List<T> list = new ArrayList<T>(this.size);
        Iterator<T> it = this.iterator();
        while (it.hasNext()) {
            list.add(it.next());
        }
        return list;
    }

    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final int ci = consumerIndex;
            private final int pi = producerIndex;
            private int s = size;
            private int i = ci;

            public boolean hasNext() {
                checkForModification();
                return s > 0;
            }

            public T next() {
                checkForModification();
                if (s == 0) throw new NoSuchElementException();

                s--;
                T r = (T) underlying[i];
                i = (i + 1) % capacity;

                return r;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void checkForModification() {
                if (ci != consumerIndex) throw new ConcurrentModificationException();
                if (pi != producerIndex) throw new ConcurrentModificationException();
            }
        };
    }
}