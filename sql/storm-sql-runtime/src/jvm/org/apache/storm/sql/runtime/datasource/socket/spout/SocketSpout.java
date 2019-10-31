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
 *
 */

package org.apache.storm.sql.runtime.datasource.socket.spout;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.storm.Config;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout for Socket data. Only available for Storm SQL.
 * The class doesn't handle reconnection, so you may not want to use this for production.
 */
public class SocketSpout implements IRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSpout.class);

    private final String host;
    private final int port;
    private final Scheme scheme;

    private volatile boolean running;

    private BlockingDeque<List<Object>> queue;
    private Socket socket;
    private Thread readerThread;
    private BufferedReader in;
    private ObjectMapper objectMapper;

    private SpoutOutputCollector collector;
    private Map<String, List<Object>> emitted;

    /**
     * SocketSpout Constructor.
     * @param scheme Scheme
     * @param host socket host
     * @param port socket port
     */
    public SocketSpout(Scheme scheme, String host, int port) {
        this.scheme = scheme;
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingDeque<>();
        this.emitted = new HashMap<>();
        this.objectMapper = new ObjectMapper();

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException("Error opening socket: host " + host + " port " + port);
        }

        readerThread = new Thread(new SocketSpout.SocketReaderRunnable());
        readerThread.start();
    }

    @Override
    public void close() {
        running = false;
        readerThread.interrupt();
        queue.clear();

        closeQuietly(in);
        closeQuietly(socket);
    }

    @Override
    public void activate() {
        running = true;
    }

    @Override
    public void deactivate() {
        running = false;
    }

    @Override
    public void nextTuple() {
        if (queue.peek() != null) {
            List<Object> values = queue.poll();
            if (values != null) {
                String id = UUID.randomUUID().toString();
                emitted.put(id, values);
                collector.emit(values, id);
            }
        }
    }

    private List<Object> convertLineToTuple(String line) {
        return scheme.deserialize(ByteBuffer.wrap(line.getBytes()));
    }

    @Override
    public void ack(Object msgId) {
        emitted.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        List<Object> emittedValues = emitted.remove(msgId);
        if (emittedValues != null) {
            queue.addLast(emittedValues);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    private class SocketReaderRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    String line = in.readLine();
                    if (line == null) {
                        throw new RuntimeException("EOF reached from the socket. We can't read the data any more.");
                    }

                    List<Object> values = convertLineToTuple(line.trim());
                    queue.push(values);
                } catch (Throwable t) {
                    // This spout is added to test purpose, so just failing fast doesn't hurt much
                    die(t);
                }
            }
        }
    }

    private void die(Throwable t) {
        LOG.error("Halting process: TridentSocketSpout died.", t);
        if (running || (t instanceof Error)) { //don't exit if not running, unless it is an Error
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