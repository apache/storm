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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import org.apache.storm.perf.ThroughputMeter;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordGenSpout extends BaseRichSpout {
    public static final String FIELDS = "word";
    private static final long serialVersionUID = -2582705611472467172L;
    private String file;
    private boolean ackEnabled = true;
    private SpoutOutputCollector collector;

    private long count = 0;
    private int index = 0;
    private ThroughputMeter emitMeter;
    private ArrayList<String> words;


    public WordGenSpout(String file) {
        this.file = file;
    }

    /**
     * Reads text file and extracts words from each line.
     *
     * @return a list of all (non-unique) words
     */
    public static ArrayList<String> readWords(String file) {
        ArrayList<String> lines = new ArrayList<>();
        try {
            FileInputStream input = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    for (String word : line.split("\\s+")) {
                        lines.add(word);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Reading file failed", e);
            } finally {
                reader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing reader", e);
        }
        return lines;
    }

    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        Integer ackers = Helper.getInt(conf, "topology.acker.executors", 0);
        if (ackers.equals(0)) {
            this.ackEnabled = false;
        }
        // for tests, reader will not be null
        words = readWords(file);
        emitMeter = new ThroughputMeter("WordGenSpout emits");
    }

    @Override
    public void nextTuple() {
        index = (index < words.size() - 1) ? index + 1 : 0;
        String word = words.get(index);
        if (ackEnabled) {
            collector.emit(new Values(word), count);
            count++;
        } else {
            collector.emit(new Values(word));
        }
        emitMeter.record();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }

}
