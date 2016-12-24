/*
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

package org.apache.storm.sql.runtime.datasource.socket.trident;

import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * StateUpdater for SocketState. Serializes tuple one by one and writes to socket, and finally flushes.
 */
public class SocketStateUpdater extends BaseStateUpdater<SocketState> {
    private static final Logger LOG = LoggerFactory.getLogger(SocketStateUpdater.class);

    private final IOutputSerializer outputSerializer;

    public SocketStateUpdater(IOutputSerializer outputSerializer) {
        this.outputSerializer = outputSerializer;
    }

    @Override
    public void updateState(SocketState state, List<TridentTuple> tuples, TridentCollector collector) {
        try {
            for (TridentTuple tuple : tuples) {
                byte[] array = outputSerializer.write(tuple.getValues(), null).array();
                String data = new String(array);
                state.write(data + "\n");
            }
            state.flush();
        } catch (IOException e) {
            LOG.error("Error while updating state.", e);
            collector.reportError(e);
            throw new RuntimeException("Error while updating state.", e);
        }

    }
}
