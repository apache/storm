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

import org.apache.storm.elasticsearch.ElasticsearchSearchRequest;
import org.apache.storm.elasticsearch.EsSearchResultOutput;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.util.Collection;

import static org.elasticsearch.common.base.Preconditions.checkNotNull;

public class EsSearchBolt extends AbstractEsBolt{

    private final ElasticsearchSearchRequest searchRequest;
    private final EsSearchResultOutput output;

    /**
     * @throws NullPointerException if any of the parameters is null
     */
    public EsSearchBolt(EsConfig esConfig, ElasticsearchSearchRequest searchRequest,
                        EsSearchResultOutput output) {
        super(esConfig);
        checkNotNull(searchRequest);
        checkNotNull(output);
        this.searchRequest = searchRequest;
        this.output = output;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            SearchRequest request = searchRequest.extractFrom(tuple);
            SearchResponse response = client.search(request).actionGet();
            Collection<Values> values = output.toValues(response);
            tryEmitAndAck(values, tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
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
