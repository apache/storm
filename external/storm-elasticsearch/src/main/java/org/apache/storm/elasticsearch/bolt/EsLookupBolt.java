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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.elasticsearch.DefaultEsLookupResultOutput;
import org.apache.storm.elasticsearch.EsLookupResultOutput;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.Response;

/**
 * Basic bolt for looking up document in ES.
 * @since 0.11
 */
public class EsLookupBolt extends AbstractEsBolt {

    private final EsTupleMapper tupleMapper;
    private final EsLookupResultOutput output;

    /**
     * EsLookupBolt constructor.
     * @param esConfig Elasticsearch configuration containing node addresses {@link EsConfig}
     * @throws NullPointerException if any of the parameters is null
     */
    public EsLookupBolt(EsConfig esConfig) {
        this(esConfig, new DefaultEsTupleMapper(), new DefaultEsLookupResultOutput(objectMapper));
    }

    /**
     * EsLookupBolt constructor.
     * @param esConfig Elasticsearch configuration containing node addresses {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     * @param output ES response to Values mapper {@link EsLookupResultOutput}
     * @throws NullPointerException if any of the parameters is null
     */
    public EsLookupBolt(EsConfig esConfig, EsTupleMapper tupleMapper, EsLookupResultOutput output) {
        super(esConfig);
        this.tupleMapper = requireNonNull(tupleMapper);
        this.output = requireNonNull(output);
    }

    @Override
    public void process(Tuple tuple) {
        try {
            Collection<Values> values = lookupValuesInEs(tuple);
            tryEmitAndAck(values, tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    private Collection<Values> lookupValuesInEs(Tuple tuple) throws IOException {
        String index = tupleMapper.getIndex(tuple);
        String type = tupleMapper.getType(tuple);
        String id = tupleMapper.getId(tuple);
        Map<String, String> params = tupleMapper.getParams(tuple, new HashMap<>());

        Response response = client.performRequest("get", getEndpoint(index, type, id), params);
        return output.toValues(response);
    }

    private void tryEmitAndAck(Collection<Values> values, Tuple tuple) {
        for (Values value : values) {
            collector.emit(tuple, value);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(output.fields());
    }
}
