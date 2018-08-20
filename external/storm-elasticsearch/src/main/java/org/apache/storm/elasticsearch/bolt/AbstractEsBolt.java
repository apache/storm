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

package org.apache.storm.elasticsearch.bolt;

import static java.util.Objects.requireNonNull;
import static org.apache.http.util.Args.notBlank;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.StormElasticSearchClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEsBolt extends BaseTickTupleAwareRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEsBolt.class);

    protected static RestClient client;
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    protected OutputCollector collector;
    private EsConfig esConfig;

    public AbstractEsBolt(EsConfig esConfig) {
        this.esConfig = requireNonNull(esConfig);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.collector = outputCollector;
            synchronized (AbstractEsBolt.class) {
                if (client == null) {
                    client = new StormElasticSearchClient(esConfig).construct();
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsBolt ", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * Construct an Elasticsearch endpoint from the provided index, type and
     * id.
     * @param index - required; name of Elasticsearch index
     * @param type - optional; name of Elasticsearch type
     * @param id - optional; Elasticsearch document ID
     * @return the index, type and id concatenated with '/'.
     */
    static String getEndpoint(String index, String type, String id) {
        requireNonNull(index);
        notBlank(index, "index");

        StringBuilder sb = new StringBuilder();
        sb.append("/").append(index);
        if (!(type == null || type.isEmpty())) {
            sb.append("/").append(type);
        }
        if (!(id == null || id.isEmpty())) {
            sb.append("/").append(id);
        }
        return sb.toString();
    }

    static RestClient getClient() {
        return AbstractEsBolt.client;
    }

    static void replaceClient(RestClient client) {
        AbstractEsBolt.client = client;
    }
}
