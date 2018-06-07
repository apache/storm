/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.windowing.persistence;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.windowing.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around the window related states that are checkpointed.
 */
public class WindowState<T> extends AbstractCollection<Event<T>> {
    // number of events per window-partition
    public static final int MAX_PARTITION_EVENTS = 1000;
    public static final int MIN_PARTITIONS = 10;
    private static final Logger LOG = LoggerFactory.getLogger(WindowState.class);
    private static final String PARTITION_IDS_KEY = "pk";
    private final KeyValueState<String, Deque<Long>> partitionIdsState;
    private final KeyValueState<Long, WindowPartition<T>> windowPartitionsState;
    private final KeyValueState<String, Optional<?>> windowSystemState;
    private final ReentrantLock partitionIdsLock = new ReentrantLock(true);
    private final WindowPartitionLock windowPartitionsLock = new WindowPartitionLock();
    private final long maxEventsInMemory;
    // ordered partition keys
    private volatile Deque<Long> partitionIds;
    private volatile long latestPartitionId;
    private volatile WindowPartition<T> latestPartition;
    private volatile WindowPartitionCache<Long, WindowPartition<T>> cache;
    private Supplier<Map<String, Optional<?>>> windowSystemStateSupplier;
    private Set<Long> iteratorPins = new HashSet<>();

    public WindowState(KeyValueState<Long, WindowPartition<T>> windowPartitionsState,
                       KeyValueState<String, Deque<Long>> partitionIdsState,
                       KeyValueState<String, Optional<?>> windowSystemState,
                       Supplier<Map<String, Optional<?>>> windowSystemStateSupplier,
                       long maxEventsInMemory) {
        this.windowPartitionsState = windowPartitionsState;
        this.partitionIdsState = partitionIdsState;
        this.windowSystemState = windowSystemState;
        this.windowSystemStateSupplier = windowSystemStateSupplier;
        this.maxEventsInMemory = Math.max(MAX_PARTITION_EVENTS * MIN_PARTITIONS, maxEventsInMemory);
        init();
    }

    @Override
    public boolean add(Event<T> event) {
        if (latestPartition.size() >= MAX_PARTITION_EVENTS) {
            cache.unpin(latestPartition.getId());
            latestPartition = getPinnedPartition(getNextPartitionId());
        }
        latestPartition.add(event);
        return true;
    }

    @Override
    public Iterator<Event<T>> iterator() {

        return new Iterator<Event<T>>() {
            private Iterator<Long> ids = getIds();
            private Iterator<Event<T>> current = Collections.emptyIterator();
            private Iterator<Event<T>> removeFrom;
            private WindowPartition<T> curPartition;

            private Iterator<Long> getIds() {
                try {
                    partitionIdsLock.lock();
                    LOG.debug("Iterator partitionIds: {}", partitionIds);
                    return new ArrayList<>(partitionIds).iterator();
                } finally {
                    partitionIdsLock.unlock();
                }
            }

            @Override
            public void remove() {
                if (removeFrom == null) {
                    throw new IllegalStateException("No calls to next() since last call to remove()");
                }
                removeFrom.remove();
                removeFrom = null;
            }

            @Override
            public boolean hasNext() {
                boolean curHasNext = current.hasNext();
                while (!curHasNext && ids.hasNext()) {
                    if (curPartition != null) {
                        unpin(curPartition.getId());
                    }
                    curPartition = getPinnedPartition(ids.next());
                    if (curPartition != null) {
                        iteratorPins.add(curPartition.getId());
                        current = curPartition.iterator();
                        curHasNext = current.hasNext();
                    }
                }
                // un-pin the last partition
                if (!curHasNext && curPartition != null) {
                    unpin(curPartition.getId());
                    curPartition = null;
                }
                return curHasNext;
            }

            @Override
            public Event<T> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                removeFrom = current;
                return current.next();
            }

            private void unpin(long id) {
                cache.unpin(id);
                iteratorPins.remove(id);
            }
        };
    }

    public void clearIteratorPins() {
        LOG.debug("clearIteratorPins '{}'", iteratorPins);
        Iterator<Long> it = iteratorPins.iterator();
        while (it.hasNext()) {
            cache.unpin(it.next());
            it.remove();
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    /**
     * Prepares the {@link WindowState} for commit.
     *
     * @param txid the transaction id
     */
    public void prepareCommit(long txid) {
        flush();
        partitionIdsState.prepareCommit(txid);
        windowPartitionsState.prepareCommit(txid);
        windowSystemState.prepareCommit(txid);
    }

    /**
     * Commits the {@link WindowState}.
     *
     * @param txid the transaction id
     */
    public void commit(long txid) {
        partitionIdsState.commit(txid);
        windowPartitionsState.commit(txid);
        windowSystemState.commit(txid);
    }

    /**
     * Rolls back the {@link WindowState}.
     *
     * @param reInit if the members should be synced with the values from the state.
     */
    public void rollback(boolean reInit) {
        partitionIdsState.rollback();
        windowPartitionsState.rollback();
        windowSystemState.rollback();
        // re-init cache and partitions
        if (reInit) {
            init();
        }
    }

    private void init() {
        initCache();
        initPartitions();
    }

    private void initPartitions() {
        partitionIds = partitionIdsState.get(PARTITION_IDS_KEY, new LinkedList<>());
        if (partitionIds.isEmpty()) {
            partitionIds.add(0L);
            partitionIdsState.put(PARTITION_IDS_KEY, partitionIds);
        }
        latestPartitionId = partitionIds.peekLast();
        latestPartition = cache.pinAndGet(latestPartitionId);
    }

    private void initCache() {
        long size = maxEventsInMemory / MAX_PARTITION_EVENTS;
        LOG.info("maxEventsInMemory: {}, partition size: {}, number of partitions: {}",
                 maxEventsInMemory, MAX_PARTITION_EVENTS, size);
        cache = SimpleWindowPartitionCache.<Long, WindowPartition<T>>newBuilder()
            .maximumSize(size)
            .removalListener(new WindowPartitionCache.RemovalListener<Long, WindowPartition<T>>() {
                @Override
                public void onRemoval(Long pid, WindowPartition<T> p, WindowPartitionCache.RemovalCause removalCause) {
                    Objects.requireNonNull(pid, "Null partition id");
                    Objects.requireNonNull(p, "Null window partition");
                    LOG.debug("onRemoval for id '{}', WindowPartition '{}'", pid, p);
                    try {
                        windowPartitionsLock.lock(pid);
                        if (p.isEmpty() && pid != latestPartitionId) {
                            // if the empty partition was not invalidated by flush, but evicted from cache
                            if (removalCause != WindowPartitionCache.RemovalCause.EXPLICIT) {
                                deletePartition(pid);
                                windowPartitionsState.delete(pid);
                            }
                        } else if (p.isModified()) {
                            windowPartitionsState.put(pid, p);
                        } else {
                            LOG.debug("WindowPartition '{}' is not modified", pid);
                        }
                    } finally {
                        windowPartitionsLock.unlock(pid);
                    }
                }
            }).build(new WindowPartitionCache.CacheLoader<Long, WindowPartition<T>>() {
                @Override
                public WindowPartition<T> load(Long id) {
                    LOG.debug("Load partition: {}", id);
                    // load from state
                    try {
                        windowPartitionsLock.lock(id);
                        return windowPartitionsState.get(id, new WindowPartition<>(id));
                    } finally {
                        windowPartitionsLock.unlock(id);
                    }
                }
            });
    }

    private void deletePartition(long pid) {
        LOG.debug("Delete partition: {}", pid);
        try {
            partitionIdsLock.lock();
            partitionIds.remove(pid);
            partitionIdsState.put(PARTITION_IDS_KEY, partitionIds);
        } finally {
            partitionIdsLock.unlock();
        }
    }

    private long getNextPartitionId() {
        try {
            partitionIdsLock.lock();
            partitionIds.add(++latestPartitionId);
            partitionIdsState.put(PARTITION_IDS_KEY, partitionIds);
        } finally {
            partitionIdsLock.unlock();
        }
        return latestPartitionId;
    }

    private WindowPartition<T> getPinnedPartition(long id) {
        return cache.pinAndGet(id);
    }

    private void flush() {
        LOG.debug("Flushing modified partitions");
        cache.asMap().forEach((pid, p) -> {
            Long pidToInvalidate = null;
            try {
                windowPartitionsLock.lock(pid);
                if (p.isEmpty() && pid != latestPartitionId) {
                    LOG.debug("Invalidating empty partition {}", pid);
                    deletePartition(pid);
                    windowPartitionsState.delete(pid);
                    pidToInvalidate = pid;
                } else if (p.isModified()) {
                    LOG.debug("Updating modified partition {}", pid);
                    p.clearModified();
                    windowPartitionsState.put(pid, p);
                }
            } finally {
                windowPartitionsLock.unlock(pid);
            }
            // invalidate after releasing the lock
            // if the parition is pinned before we could invalidate,
            // it will get invalidated in the next flush or when the entry gets evicted from the cache.
            if (pidToInvalidate != null) {
                cache.invalidate(pidToInvalidate);
            }
        });
        Map<String, Optional<?>> state = windowSystemStateSupplier.get();
        for (Map.Entry<String, Optional<?>> entry : state.entrySet()) {
            windowSystemState.put(entry.getKey(), entry.getValue());
        }
    }

    private static class WindowPartitionLock {
        private final int numLocks = 8;
        private final ImmutableMap<Long, ReentrantLock> locks;

        WindowPartitionLock() {
            ImmutableMap.Builder<Long, ReentrantLock> builder = ImmutableMap.builder();
            for (long i = 0; i < numLocks; i++) {
                builder.put(i, new ReentrantLock(true));
            }
            locks = builder.build();
        }

        private void lock(long i) {
            locks.get(i % numLocks).lock();
        }

        private void unlock(long i) {
            locks.get(i % numLocks).unlock();
        }
    }

    // the window partition that holds the events
    public static class WindowPartition<T> implements Iterable<Event<T>> {
        private final ConcurrentLinkedQueue<Event<T>> events = new ConcurrentLinkedQueue<>();
        private final AtomicInteger size = new AtomicInteger();
        private final long id;
        private transient volatile boolean modified;

        public WindowPartition(long id) {
            this.id = id;
        }

        void add(Event<T> event) {
            events.add(event);
            size.incrementAndGet();
            setModified();
        }

        boolean isModified() {
            return modified;
        }

        void setModified() {
            if (!modified) {
                modified = true;
            }
        }

        void clearModified() {
            modified = false;
        }

        boolean isEmpty() {
            return events.isEmpty();
        }

        @Override
        public Iterator<Event<T>> iterator() {
            return new Iterator<Event<T>>() {
                Iterator<Event<T>> it = events.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Event<T> next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    it.remove();
                    size.decrementAndGet();
                    setModified();
                }
            };
        }

        public int size() {
            return size.get();
        }

        public long getId() {
            return id;
        }

        // for unit tests
        public Collection<Event<T>> getEvents() {
            return Collections.unmodifiableCollection(events);
        }

        @Override
        public String toString() {
            return "WindowPartition{id=" + id + ", size=" + size + '}';
        }
    }
}
