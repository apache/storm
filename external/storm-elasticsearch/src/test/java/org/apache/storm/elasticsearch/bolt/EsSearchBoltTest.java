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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EsSearchBoltTest extends AbstractEsBoltTest<EsSearchBolt> {

    @Mock
    private EsConfig esConfig;

    @Mock
    private Tuple tuple;

    @Mock
    private Client client;

    @Mock
    private ElasticsearchSearchRequest searchRequest;

    @Mock
    private SearchRequest request;

    @Mock
    private EsSearchResultOutput output;

    private Client originalClient;


    @Override
    protected EsSearchBolt createBolt(EsConfig esConfig) {
        originalClient = EsLookupBolt.getClient();
        EsLookupBolt.replaceClient(this.client);
        return new EsSearchBolt(esConfig, searchRequest, output);
    }

    @Before
    public void configureBoltDependencies() throws Exception {
        when(searchRequest.extractFrom(tuple)).thenReturn(request);
        when(output.toValues(any(SearchResponse.class))).thenReturn(Collections.singleton(new Values("")));
    }

    @After
    public void replaceClientWithOriginal() throws Exception {
        EsLookupBolt.replaceClient(originalClient);
    }


    @Test
    public void failsTupleWhenClientThrows() throws Exception {
        when(client.search(request)).thenThrow(ElasticsearchException.class);
        bolt.execute(tuple);

        verify(outputCollector).fail(tuple);
    }

    @Test
    public void reportsExceptionWhenClientThrows() throws Exception {
        ElasticsearchException elasticsearchException = new ElasticsearchException("dummy");
        when(client.search(request)).thenThrow(elasticsearchException);
        bolt.execute(tuple);

        verify(outputCollector).reportError(elasticsearchException);
    }

    @Override
    protected Class<EsSearchBolt> getBoltClass() {
        return EsSearchBolt.class;
    }
}
