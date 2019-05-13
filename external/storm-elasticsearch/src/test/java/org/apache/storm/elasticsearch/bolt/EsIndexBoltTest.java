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

import static org.mockito.Mockito.verify;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class EsIndexBoltTest extends AbstractEsBoltIntegrationTest<EsIndexBolt> {

    @Test
    public void testEsIndexBolt()
            throws Exception {
        Tuple tuple = createTestTuple(index, type);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        SearchResponse resp = node.client().prepareSearch(index)
                .setQuery(new TermQueryBuilder("_type", type))
                .setSize(0)
                .execute().actionGet();

        Assert.assertEquals(1, resp.getHits().getTotalHits());
    }

    @Test
    public void indexMissing()
            throws Exception {
        String index = "missing";

        Tuple tuple = createTestTuple(index, type);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        SearchResponse resp = node.client().prepareSearch(index)
                .setQuery(new TermQueryBuilder("_type", type))
                .setSize(0)
                .execute().actionGet();

        Assert.assertEquals(1, resp.getHits().getTotalHits());
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
