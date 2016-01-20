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
package org.apache.storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * A partition state manager that simply encapsulates a partition in itself. Each instance of this manager keeps
 * the state of its corresponding partition.
 */
public class PartitionStateManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateStore.class);

    private Partition _partition;
    private StateStore _stateStore;

    public PartitionStateManager(Partition partition, StateStore stateStore) {
        this._partition = partition;
        this._stateStore = stateStore;
    }

    public Map<Object, Object> getState() {
        return _stateStore.readState(_partition);
    }

    public void writeState(Map<Object, Object> state) {
        _stateStore.writeState(_partition, state);
    }

    @Override
    public void close() throws IOException {
        if (_stateStore != null) {
            _stateStore.close();
        }
        LOG.info("State store closed.");
    }
}