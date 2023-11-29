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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class EsIndexBoltTest extends AbstractEsBoltIntegrationTest<EsIndexBolt> {

    @Test
    public void testEsIndexBolt() throws IOException {
        Tuple tuple = createTestTuple(index, type);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);

        RestHighLevelClient client =  EsTestUtil.getRestHighLevelClient(node);
        RefreshRequest request = new RefreshRequest(index);
        client.indices().refresh(request, RequestOptions.DEFAULT);

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new TermQueryBuilder("_type", type));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);

        SearchResponse resp = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1, resp.getHits().getTotalHits());
    }

    @Test
    public void indexMissing() throws IOException {
        String index = "missing";

        Tuple tuple = createTestTuple(index, type);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);

        RestHighLevelClient client =  EsTestUtil.getRestHighLevelClient(node);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new TermQueryBuilder("_type", type));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);

        SearchResponse resp = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1, resp.getHits().getTotalHits());
    }

    private Tuple createTestTuple(String index, String type) {
        return EsTestUtil.generateTestTuple(source, index, type, documentId);
    }

    @Override
    protected EsIndexBolt createBolt(EsConfig esConfig) {
        return new EsIndexBolt(esConfig);
    }

    @Override
    protected Class<EsIndexBolt> getBoltClass() {
        return EsIndexBolt.class;
    }
}
