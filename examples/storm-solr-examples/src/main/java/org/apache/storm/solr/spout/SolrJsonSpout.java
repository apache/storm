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

package org.apache.storm.solr.spout;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.solr.util.TestUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SolrJsonSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final List<Values> listValues = Lists.newArrayList(
            getJsonValues("1"), getJsonValues("2"), getJsonValues("3"), // Tuple contains String Object in JSON format
            getPojoValues("1"), getPojoValues("2"));    // Tuple contains Java object that must be serialized to JSON by SolrJsonMapper

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final Values values = listValues.get(rand.nextInt(listValues.size()));
        collector.emit(values);
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields("JSON");
    }

    @Override
    public void close() {   //TODO
        super.close();
    }

    // ====

    private static Values getJsonValues(String suf) {
        String suffix = "_json_test_val_" + suf;
        return new Values((new JsonSchema(suffix)).toJson());
    }

    private static Values getPojoValues(String suf) {
        String suffix = "_json_test_val_" + suf;
        return new Values(new JsonSchema(suffix));
    }

    public static class JsonSchema {
        private String id;
        private String date;
        private String dcTitle;

        private static final Gson gson = new Gson();

        public JsonSchema(String suffix) {
            this.id = "id" + suffix;
            this.date = TestUtil.getDate();
            this.dcTitle = "dcTitle" + suffix;
        }

        public JsonSchema(String id, String date, String dcTitle) {
            this.id = id;
            this.date = date;
            this.dcTitle = dcTitle;
        }

        // copy constructor
        public JsonSchema(JsonSchema jsonSchema) {
            this.id = jsonSchema.id;
            this.date = jsonSchema.date;
            this.dcTitle = jsonSchema.dcTitle;
        }

        public String toJson() {
            String json = gson.toJson(this);
            System.out.println(json);   // TODO log
            return json;
        }

        public static JsonSchema fromJson(String jsonStr) {
            return new JsonSchema(gson.fromJson(jsonStr, JsonSchema.class));
        }
    }
}
