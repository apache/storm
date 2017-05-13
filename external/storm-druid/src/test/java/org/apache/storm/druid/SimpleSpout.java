/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.druid;

import org.apache.storm.druid.bolt.TupleDruidEventMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.joda.time.DateTime;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    int i = 1;

    public static final Fields DEFAULT_FIELDS = new Fields(TupleDruidEventMapper.DEFAULT_FIELD_NAME);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("timestamp", new DateTime().toString());
        event.put("publisher", "foo.com");
        event.put("advertiser", "google.com");
        event.put("click", i++);
        _collector.emit(new Values(event));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(DEFAULT_FIELDS);
    }

}
