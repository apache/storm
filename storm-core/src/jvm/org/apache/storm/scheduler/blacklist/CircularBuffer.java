package org.apache.storm.scheduler.blacklist;

/**
 * Created by howard.li on 2016/5/19.
 */

import java.io.Serializable;
import java.util.*;

public final class CircularBuffer<T extends Serializable> extends AbstractCollection<T> implements Serializable {

    // This is the largest capacity allowed by this implementation
    private static final int MAX_CAPACITY = 1 << 30;
    private static final int DEFAULT_CAPACITY = 1 << 8;

    private int size          = 0;
    private int producerIndex = 0;
    private int consumerIndex = 0;

    // capacity must be a power of 2 at all times
    private int capacity;
    //private int maxCapacity;

    // we mask with capacity -1.  This variable caches that values
    //private int bitmask;

    private Serializable[] q;

    public CircularBuffer() {
        this(DEFAULT_CAPACITY);
    }

    // Construct a buffer which has at least the specified capacity.  If
    // the value specified is a power of two then the buffer will be
    // exactly the specified size.  Otherwise the buffer will be the
    // first power of two which is greater than the specified value.
    public CircularBuffer(int c) {

        if (c > MAX_CAPACITY) {
            throw new IllegalArgumentException("Capacity greater than " +
                    "allowed");
        }

        //for (capacity = 1; capacity < c; capacity <<= 1) ;
        capacity=c;
        //bitmask = capacity - 1;
        q = new Serializable[capacity];
    }

    // Constructor used by clone()
    private CircularBuffer(CircularBuffer oldBuffer) {
        size = oldBuffer.size;
        producerIndex = oldBuffer.producerIndex;
        consumerIndex = oldBuffer.consumerIndex;
        capacity = oldBuffer.capacity;
        //bitmask = oldBuffer.bitmask;
        q = new Serializable[oldBuffer.q.length];
        System.arraycopy(oldBuffer.q, 0, q, 0, q.length);
    }

    private boolean isFull(){
        return size==capacity;
    }

    public boolean add(Serializable obj) {
        if (isFull()) {
            remove();
        }

        size++;
        q[producerIndex] = obj;

        producerIndex = (producerIndex + 1) % capacity;

        return true;
    }

    public T remove() {
        Serializable obj;

        if (size == 0) return null;

        size--;
        obj = q[consumerIndex];
        q[consumerIndex] = null; // allow gc to collect

        consumerIndex = (consumerIndex + 1) % capacity;

        return (T)obj;
    }

    public boolean isEmpty() { return size == 0; }

    public int size() { return size; }

    public int capacity() { return capacity; }

    public Serializable peek() {
        if (size == 0) return null;
        return q[consumerIndex];
    }

    public void clear() {
        Arrays.fill(q, null);
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

    public Iterator iterator() {
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
                T r = (T)q[i];
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