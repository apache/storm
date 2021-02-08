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

package org.apache.storm.windowing;

import static org.apache.storm.windowing.EvictionPolicy.Action.EXPIRE;
import static org.apache.storm.windowing.EvictionPolicy.Action.PROCESS;
import static org.apache.storm.windowing.EvictionPolicy.Action.STOP;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Window manager that handles windows with state persistence.
 */
public class StatefulWindowManager<T> extends WindowManager<T> {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowManager.class);

    public StatefulWindowManager(WindowLifecycleListener<T> lifecycleListener) {
        super(lifecycleListener);
    }

    /**
     * Constructs a {@link StatefulWindowManager}.
     *
     * @param lifecycleListener the {@link WindowLifecycleListener}
     * @param queue             a collection where the events in the window can be enqueued. <br/>
     *                          <b>Note:</b> This collection has to be thread safe.
     */
    public StatefulWindowManager(WindowLifecycleListener<T> lifecycleListener, Collection<Event<T>> queue) {
        super(lifecycleListener, queue);
    }

    private static <T> Iterator<T> expiringIterator(Iterator<T> inner, IteratorStatus status) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                if (status.isValid()) {
                    return inner.hasNext();
                }
                throw new IllegalStateException("Stale iterator, the iterator is valid only within the corresponding execute");
            }

            @Override
            public T next() {
                if (status.isValid()) {
                    return inner.next();
                }
                throw new IllegalStateException("Stale iterator, the iterator is valid only within the corresponding execute");
            }
        };
    }

    @Override
    protected void compactWindow() {
        // NOOP
    }

    @Override
    public boolean onTrigger() {
        Supplier<Iterator<T>> scanEventsStateful = this::scanEventsStateful;
        Iterator<T> it = scanEventsStateful.get();
        boolean hasEvents = it.hasNext();
        if (hasEvents) {
            final IteratorStatus status = new IteratorStatus();
            LOG.debug("invoking windowLifecycleListener onActivation with iterator");
            // reuse the retrieved iterator
            Supplier<Iterator<T>> wrapper = new Supplier<Iterator<T>>() {
                Iterator<T> initial = it;

                @Override
                public Iterator<T> get() {
                    if (status.isValid()) {
                        Iterator<T> res;
                        if (initial != null) {
                            res = initial;
                            initial = null;
                        } else {
                            res = scanEventsStateful.get();
                        }
                        return expiringIterator(res, status);
                    }
                    throw new IllegalStateException("Stale window, the window is valid only within the corresponding execute");
                }
            };
            windowLifecycleListener.onActivation(wrapper, null, null, evictionPolicy.getContext().getReferenceTime());
            // invalidate the iterator
            status.invalidate();
        } else {
            LOG.debug("No events in the window, skipping onActivation");
        }
        triggerPolicy.reset();
        return hasEvents;
    }

    private Iterator<T> scanEventsStateful() {
        LOG.debug("Scan events, eviction policy {}", evictionPolicy);
        evictionPolicy.reset();
        Iterator<T> it = new Iterator<T>() {
            private Iterator<Event<T>> inner = queue.iterator();
            private T windowEvent;
            private boolean stopped;

            @Override
            public boolean hasNext() {
                while (!stopped && windowEvent == null && inner.hasNext()) {
                    Event<T> cur = inner.next();
                    EvictionPolicy.Action action = evictionPolicy.evict(cur);
                    if (action == EXPIRE) {
                        inner.remove();
                    } else if (action == STOP) {
                        stopped = true;
                    } else if (action == PROCESS) {
                        windowEvent = cur.get();
                    }
                }
                return windowEvent != null;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                T res = windowEvent;
                windowEvent = null;
                return res;
            }
        };

        return it;

    }

    private static class IteratorStatus {
        private boolean valid = true;

        void invalidate() {
            valid = false;
        }

        boolean isValid() {
            return valid;
        }
    }
}
