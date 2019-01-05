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
package org.apache.storm.elasticsearch.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.node.Node;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@IntegrationTest
@ExtendWith(MockitoExtension.class)
public class EsStateTest {
    
    private static Node node;

    private final String[] documentId = {UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    private final String index = "index";
    private final String type = "type";
    private final String[] source = {"{\"user\":\"user1\"}", "{\"user\":\"user1\"}"};

    private EsState state = createEsState();
    
    @BeforeAll
    public static void startElasticSearchNode() throws Exception {
        node = EsTestUtil.startEsNode();
        EsTestUtil.ensureEsGreen(node);
    }

    @AfterAll
    public static void closeElasticSearchNode() throws Exception {
        EsTestUtil.stopEsNode(node);
    }
    
    @AfterEach
    public void clearIndex() throws Exception {
        EsTestUtil.clearIndex(node, index);
        EsTestUtil.clearIndex(node, "missing");
    }
    
    private EsState createEsState() {
        EsState state = new EsState(esConfig(), new DefaultEsTupleMapper());
        state.prepare();
        return state;
    }

    private EsConfig esConfig() {
        return new EsConfig();
    }
    
    private List<TridentTuple> tuples(String index, String type, String[] ids, String[] sources) {
        List<TridentTuple> tuples = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            tuples.add(EsTestUtil.generateTestTridentTuple(sources[i], index, type, ids[i]));
        }
        return tuples;
    }

    @Test
    public void updateState() throws Exception {
        List<TridentTuple> tuples = tuples(index, type, documentId, source);
        state.updateState(tuples);
    }

    @Test
    public void indexMissing() throws Exception {
        List<TridentTuple> tuples = tuples("missing", type, documentId, source);
        state.updateState(tuples);
    }
}
