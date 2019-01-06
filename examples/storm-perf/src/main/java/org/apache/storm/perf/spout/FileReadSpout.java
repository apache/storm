/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf.spout;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileReadSpout extends BaseRichSpout {
    public static final String FIELDS = "sentence";
    private static final long serialVersionUID = -2582705611472467172L;
    private transient BufferOnInitFileReader reader;
    private String file;
    private boolean ackEnabled = true;
    private SpoutOutputCollector collector;

    private long count = 0;


    public FileReadSpout(String file) {
        this.file = file;
    }

    // For testing
    FileReadSpout(BufferOnInitFileReader reader) {
        this.reader = reader;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        Object ackObj = conf.get("topology.acker.executors");
        if (ackObj != null && ackObj.equals(0)) {
            this.ackEnabled = false;
        }
        // for tests, reader will not be null
        if (this.reader == null) {
            this.reader = new BufferOnInitFileReader(this.file);
        }
    }

    @Override
    public void nextTuple() {
        if (ackEnabled) {
            collector.emit(new Values(reader.nextLine()), count);
            count++;
        } else {
            collector.emit(new Values(reader.nextLine()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }

    public static class BufferOnInitFileReader implements Serializable {

        private static final long serialVersionUID = -7012334600647556267L;

        public final String file;
        private List<String> contents = null;
        private int index = 0;
        private int limit = 0;

        public BufferOnInitFileReader(String file) {
            this.file = file;
            if (this.file != null) {
                try {
                    this.contents = Files.readAllLines(Paths.get(this.file), Charset.defaultCharset());
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException("Cannot open file " + file, e);
                }
                this.limit = contents.size();
            } else {
                throw new IllegalArgumentException("file name cannot be null");
            }
        }

        public String nextLine() {
            if (index >= limit) {
                index = 0;
            }
            String line = contents.get(index);
            index++;
            return line;
        }

    }
}
