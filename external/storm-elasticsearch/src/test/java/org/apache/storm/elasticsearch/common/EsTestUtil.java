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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static Node startEsNode(){
        Node node = NodeBuilder.nodeBuilder().data(true).settings(
                Settings.settingsBuilder()
                        .put(ClusterName.SETTING, EsConstants.clusterName)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", true)
                        .put("index.percolator.map_unmapped_fields_as_string", true)
                        .put("index.store.type", "mmapfs")
                        .put("path.home", "./data")
        ).build();
        node.start();
        return node;
    }

    public static void ensureEsGreen(Node node) {
        ClusterHealthResponse chr = node.client()
                                        .admin()
                                        .cluster()
                                        .health(Requests.clusterHealthRequest()
                                                        .timeout(TimeValue.timeValueSeconds(30))
                                                        .waitForGreenStatus()
                                                        .waitForEvents(Priority.LANGUID)
                                                        .waitForRelocatingShards(0))
                                        .actionGet();
        assertThat("cluster status is green", chr.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    /**
     * Stop the given Elasticsearch node and clear the data directory.
     * @param node
     */
    public static void stopEsNode(Node node) {
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
    public static void clearIndex(Node node, String index) {
        try {
            node.client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
        } catch (IndexNotFoundException ignore) {

        }
    }
}
