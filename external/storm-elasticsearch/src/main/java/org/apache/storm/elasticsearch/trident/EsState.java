/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.elasticsearch.trident;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.http.entity.StringEntity;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.elasticsearch.common.StormElasticSearchClient;
import org.apache.storm.elasticsearch.doc.Index;
import org.apache.storm.elasticsearch.doc.IndexDoc;
import org.apache.storm.elasticsearch.doc.SourceDoc;
import org.apache.storm.elasticsearch.response.BulkIndexResponse;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trident State for storing tuple to ES document.
 * @since 0.11
 */
class EsState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);
    private static RestClient client;
    private EsConfig esConfig;
    private final ObjectMapper objectMapper;
    private EsTupleMapper tupleMapper;

    /**
     * EsState constructor.
     *
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    EsState(EsConfig esConfig, EsTupleMapper tupleMapper) {
        this.esConfig = esConfig;
        this.objectMapper = new ObjectMapper();
        this.tupleMapper = tupleMapper;
    }

    /**
     * Elasticsearch index requests with same id will result in update operation
     * which means if same tuple replays, only one record will be stored in elasticsearch for same document
     * without control with txid.
     */
    @Override
    public void beginCommit(Long txid) {

    }

    /**
     * Elasticsearch index requests with same id will result in update operation
     * which means if same tuple replays, only one record will be stored in elasticsearch for same document
     * without control with txid.
     */
    @Override
    public void commit(Long txid) {

    }

    public void prepare() {
        try {
            synchronized (EsState.class) {
                if (client == null) {
                    client = new StormElasticSearchClient(esConfig).construct();
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsState ", e);
        }
    }

    private String buildRequest(List<TridentTuple> tuples) throws JsonProcessingException {
        StringBuilder bulkRequest = new StringBuilder();
        for (TridentTuple tuple : tuples) {
            String source = tupleMapper.getSource(tuple);
            String index = tupleMapper.getIndex(tuple);
            String type = tupleMapper.getType(tuple);
            String id = tupleMapper.getId(tuple);

            IndexDoc indexDoc = new IndexDoc(new Index(index, type, id));
            SourceDoc sourceDoc = new SourceDoc(source);
            bulkRequest.append(objectMapper.writeValueAsString(indexDoc)).append('\n');
            bulkRequest.append(objectMapper.writeValueAsString(sourceDoc)).append('\n');

        }
        return bulkRequest.toString();
    }

    /**
     * Store current state to ElasticSearch.
     *
     * @param tuples list of tuples for storing to ES.
     *               Each tuple should have relevant fields (source, index, type, id) for EsState's tupleMapper to extract ES document.
     */
    public void updateState(List<TridentTuple> tuples) {
        try {
            String bulkRequest = buildRequest(tuples);
            Response response = client.performRequest("post", "_bulk", new HashMap<>(), new StringEntity(bulkRequest.toString()));
            BulkIndexResponse bulkResponse = objectMapper.readValue(response.getEntity().getContent(), BulkIndexResponse.class);
            if (bulkResponse.hasErrors()) {
                LOG.warn("failed processing bulk index requests: " + bulkResponse.getFirstError() + ": " + bulkResponse.getFirstResult());
                throw new FailedException();
            }
        } catch (IOException e) {
            LOG.warn("failed processing bulk index requests: " + e.toString());
            throw new FailedException(e);
        }
    }
}
