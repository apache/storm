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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.response.PercolateResponse;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

public class EsPercolateBoltTest extends AbstractEsBoltIntegrationTest<EsPercolateBolt> {

    private final String source = "{\"user\":\"user1\"}";

    @Override
    protected EsPercolateBolt createBolt(EsConfig esConfig) {
        return new EsPercolateBolt(esConfig);
    }

    @BeforeEach
    public void populateIndexWithTestData() throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, ".percolator", documentId)
                .source(source, XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE); // setRefresh(true) in Elasticsearch 6.x

        RestHighLevelClient client =  EsTestUtil.getRestHighLevelClient(node);

        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testEsPercolateBolt() {
        Tuple tuple = EsTestUtil.generateTestTuple(source, index, type, null);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);
        ArgumentCaptor<Values> emitCaptor = ArgumentCaptor.forClass(Values.class);
        verify(outputCollector).emit(emitCaptor.capture());
        assertThat(emitCaptor.getValue().get(0), is(source));
        assertThat(emitCaptor.getValue().get(1), instanceOf(PercolateResponse.Match.class));
    }

    @Test
    public void noDocumentsMatch() {
        Tuple tuple = EsTestUtil.generateTestTuple("{\"user\":\"user2\"}", index, type, null);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);
        verify(outputCollector, never()).emit(any(Values.class));
    }

    @Test
    public void indexMissing() {
        String index = "missing";

        Tuple tuple = EsTestUtil.generateTestTuple(source, index, type, null);

        bolt.execute(tuple);

        verify(outputCollector, never()).emit(any(Values.class));
        verify(outputCollector).reportError(any(ResponseException.class));
        verify(outputCollector).fail(tuple);
    }

    @Override
    protected Class<EsPercolateBolt> getBoltClass() {
        return EsPercolateBolt.class;
    }
}
