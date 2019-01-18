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

package org.apache.storm.elasticsearch.bolt;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.entity.StringEntity;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.elasticsearch.response.PercolateResponse;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.Response;

/**
 * Basic bolt for retrieve matched percolate queries.
 */
public class EsPercolateBolt extends AbstractEsBolt {

    private final EsTupleMapper tupleMapper;

    /**
     * EsPercolateBolt constructor.
     * @param esConfig Elasticsearch configuration containing node addresses {@link EsConfig}
     */
    public EsPercolateBolt(EsConfig esConfig) {
        this(esConfig, new DefaultEsTupleMapper());
    }

    /**
     * EsPercolateBolt constructor.
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    public EsPercolateBolt(EsConfig esConfig, EsTupleMapper tupleMapper) {
        super(esConfig);
        this.tupleMapper = requireNonNull(tupleMapper);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }

    /**
     * {@inheritDoc}
     * Tuple should have relevant fields (source, index, type) for storeMapper to extract ES document.<br/>
     * If there exists non-empty percolate response, EsPercolateBolt will emit tuple with original source
     * and Percolate.Match for each Percolate.Match in PercolateResponse.
     */
    @Override
    public void process(Tuple tuple) {
        try {
            String source = tupleMapper.getSource(tuple);
            String index = tupleMapper.getIndex(tuple);
            String type = tupleMapper.getType(tuple);

            Map<String, String> indexParams = new HashMap<>();
            indexParams.put(type, null);
            String percolateDoc = "{\"doc\": " + source + "}";
            Response response = client.performRequest("get", getEndpoint(index, type, "_percolate"),
                    new HashMap<>(), new StringEntity(percolateDoc));
            PercolateResponse percolateResponse = objectMapper.readValue(response.getEntity().getContent(), PercolateResponse.class);
            if (!percolateResponse.getMatches().isEmpty()) {
                for (PercolateResponse.Match match : percolateResponse.getMatches()) {
                    collector.emit(new Values(source, match));
                }
            }
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "match"));
    }
}
