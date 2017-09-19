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

package org.apache.storm.loadgen;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of topology names that will be killed when this is closed, or when the
 * program exits.
 */
public class ScopedTopologySet extends HashSet<String> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ScopedTopologySet.class);
    private static final KillOptions NO_WAIT_KILL = new KillOptions();

    static {
        NO_WAIT_KILL.set_wait_secs(0);
    }

    private final Nimbus.Iface client;
    private boolean closed = false;

    /**
     * Constructor.
     * @param client the client used to kill the topologies when this exist.
     */
    public ScopedTopologySet(Nimbus.Iface client) {
        this.client = client;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error trying to shutdown topologies on exit", e);
            }
        }));
    }

    @Override
    public boolean remove(Object o) {
        throw new RuntimeException("Unmodifiable Set");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Unmodifiable Set");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("Unmodifiable Set");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("Unmodifiable Set");
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        RuntimeException saved = null;
        for (Iterator<String> it = super.iterator(); it.hasNext();) {
            String name = it.next();
            try {
                client.killTopologyWithOpts(name, NO_WAIT_KILL);
                it.remove();
            } catch (Exception e) {
                RuntimeException wrapped = new RuntimeException("Error trying to kill " + name, e);
                if (saved != null) {
                    saved.addSuppressed(wrapped);
                } else {
                    saved = wrapped;
                }
            }
        }
        super.clear();
        if (saved != null) {
            throw saved;
        }
        closed = true;
    }
}
