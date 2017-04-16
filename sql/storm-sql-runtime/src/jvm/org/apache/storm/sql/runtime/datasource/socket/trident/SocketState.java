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

package org.apache.storm.sql.runtime.datasource.socket.trident;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Trident State implementation of Socket. It only supports writing.
 */
public class SocketState implements State {
    /**
     * {@inheritDoc}
     */
    @Override
    public void beginCommit(Long txid) {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(Long txid) {
        // no-op
    }

    public static class Factory implements StateFactory {
        private final String host;
        private final int port;

        public Factory(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            BufferedWriter out;
            try {
                Socket socket = new Socket(host, port);
                out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            } catch (IOException e) {
                throw new RuntimeException("Exception while initializing socket for State. host " +
                        host + " port " + port, e);
            }

            // State doesn't have close() and Storm actually doesn't guarantee so we can't release socket resource anyway
            return new SocketState(out);
        }
    }

    private BufferedWriter out;

    private SocketState(BufferedWriter out) {
        this.out = out;
    }

    public void write(String str) throws IOException {
        out.write(str);
    }

    public void flush() throws IOException {
        out.flush();
    }
}