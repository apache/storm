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
package org.apache.storm.elasticsearch.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class EsTestUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EsTestUtil.class);

    public static Tuple generateTestTuple(String source, String index, String type, String id) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap<>(), new HashMap<>(), new HashMap<>(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("source", "index", "type", "id");
            }
        };
        return new TupleImpl(topologyContext, new Values(source, index, type, id), source, 1, "");
    }

    public static TridentTuple generateTestTridentTuple(String source, String index, String type, String id) {
        Fields fields = new Fields("source", "index", "type", "id");
        return TridentTupleView.createFreshTuple(fields,  new Values(source, index, type, id));
    }

    /**
     * Generate a mock {@link ResponseException}.
     * @return a mock {@link ResponseException}.
     * @throws IOException
     */
    public static ResponseException generateMockResponseException() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getRequestLine()).thenReturn(requestLine);
        when(response.getStatusLine()).thenReturn(statusLine);
        return new ResponseException(response);
    }

    public static RestHighLevelClient getRestHighLevelClient(ElasticsearchContainer node) {
        final EsConfig cfg = new EsConfig(node.getHttpHostAddress());
        return new RestHighLevelClientBuilder(new StormElasticSearchClient(cfg).construct()).build();
    }

    public static ElasticsearchContainer startEsNode() {
        String version = System.getProperty("elasticsearch-version");
        if (version == null) version = "7.17.13";
        LOG.info("Starting docker instance of Elasticsearch {}...", version);

        final ElasticsearchContainer container =
                new ElasticsearchContainer(
                        "docker.elastic.co/elasticsearch/elasticsearch:" + version);
        container.start();
        return container;
    }

    public static void ensureEsGreen(ElasticsearchContainer node) {
        assertThat("cluster status is green", node.isHealthy());
    }

    /**
     * Stop the given Elasticsearch node and clear the data directory.
     * @param node
     */
    public static void stopEsNode(ElasticsearchContainer node) {
        node.close();
        try {
            FileUtils.deleteDirectory(new File("./data"));
        } catch (IOException e) {
            LOG.warn(e.toString());
        }
    }

    /**
     * Clear the given index if it exists
     * @param node - the node to connect to
     * @param index - the index to clear
     */
    public static void clearIndex(ElasticsearchContainer node, String index) {
        try {
            getRestHighLevelClient(node).indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        } catch (IOException ignore) {

        }
    }
}
