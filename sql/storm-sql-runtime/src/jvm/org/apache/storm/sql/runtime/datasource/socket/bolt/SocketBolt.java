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

package org.apache.storm.sql.runtime.datasource.socket.bolt;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Bolt implementation for Socket. Only available for Storm SQL.
 * The class doesn't handle reconnection so you may not want to use this for production.
 */
public class SocketBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SocketBolt.class);

    private final IOutputSerializer serializer;
    private final String host;
    private final int port;

    private transient OutputCollector collector;
    private transient BufferedWriter writer;
    private transient Socket socket;

    /**
     * Constructor.
     */
    public SocketBolt(IOutputSerializer serializer, String host, int port) {
        this.serializer = serializer;
        this.host = host;
        this.port = port;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        try {
            this.socket = new Socket(host, port);
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
            throw new RuntimeException("Exception while initializing socket for State. host "
                    + host + " port " + port, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        Values values = (Values) input.getValue(0);
        byte[] array = serializer.write(values, null).array();
        String data = new String(array);

        try {
            writer.write(data + "\n");
            writer.flush();
            collector.ack(input);
        } catch (IOException e) {
            LOG.error("Error while writing data to socket.", e);
            collector.reportError(e);
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(socket);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}