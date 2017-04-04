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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.Config;
import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Trident Spout for Socket data. Only available for Storm SQL, and only use for test purposes.
 */
public class TridentSocketSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TridentSocketSpout.class);

    private final String host;
    private final int port;
    private final Scheme scheme;

    private volatile boolean _running = true;

    private BlockingDeque<String> queue;
    private Socket socket;
    private Thread readerThread;
    private BufferedReader in;
    private ObjectMapper objectMapper;

    private Map<Long, List<List<Object>>> batches;

    public TridentSocketSpout(Scheme scheme, String host, int port) {
        this.scheme = scheme;
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        this.queue = new LinkedBlockingDeque<>();
        this.objectMapper = new ObjectMapper();
        this.batches = new HashMap<>();

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException("Error opening socket: host " + host + " port " + port);
        }

        readerThread = new Thread(new SocketReaderRunnable());
        readerThread.start();
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        // read line and parse it to json
        List<List<Object>> batch = this.batches.get(batchId);
        if (batch == null) {
            batch = new ArrayList<>();

            while(queue.peek() != null) {
                String line = queue.poll();
                List<Object> values = convertLineToTuple(line);
                if (values == null) {
                    continue;
                }

                batch.add(values);
            }

            this.batches.put(batchId, batch);
        }

        for (List<Object> list : batch) {
            collector.emit(list);
        }
    }

    private List<Object> convertLineToTuple(String line) {
        return scheme.deserialize(ByteBuffer.wrap(line.getBytes()));
    }

    @Override
    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    @Override
    public void close() {
        _running = false;
        readerThread.interrupt();
        queue.clear();

        closeQuietly(in);
        closeQuietly(socket);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return scheme.getOutputFields();
    }

    private class SocketReaderRunnable implements Runnable {
        public void run() {
            while (_running) {
                try {
                    String line = in.readLine();
                    if (line == null) {
                        throw new RuntimeException("EOF reached from the socket. We can't read the data any more.");
                    }

                    queue.push(line.trim());
                } catch (Throwable t) {
                    // This spout is added to test purpose, so just failing fast doesn't hurt much
                    die(t);
                }
            }
        }
    }

    private void die(Throwable t) {
        LOG.error("Halting process: TridentSocketSpout died.", t);
        if (_running || (t instanceof Error)) { //don't exit if not running, unless it is an Error
            System.exit(11);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final IOException ioe) {
            // ignore
        }
    }
}
