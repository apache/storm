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

import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.testing.IntegrationTest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.node.Node;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

@IntegrationTest
public abstract class AbstractEsBoltIntegrationTest<Bolt extends AbstractEsBolt> extends AbstractEsBoltTest<Bolt> {

    protected static Node node;

    @BeforeAll
    public static void startElasticSearchNode() throws Exception {
        node = EsTestUtil.startEsNode();
        EsTestUtil.ensureEsGreen(node);
    }

    @AfterAll
    public static void closeElasticSearchNode() throws Exception {
        EsTestUtil.stopEsNode(node);
    }

    @BeforeEach
    public void createIndex() {
        node.client().admin().indices().create(new CreateIndexRequest(index)).actionGet();
    }

    @AfterEach
    public void clearIndex() throws Exception {
        EsTestUtil.clearIndex(node, index);
        EsTestUtil.clearIndex(node, "missing");
    }
}
