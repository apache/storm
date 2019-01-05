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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.ResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EsLookupBoltIntegrationTest extends AbstractEsBoltIntegrationTest<EsLookupBolt> {

    @Captor
    private ArgumentCaptor<Tuple> anchor;

    @Captor
    private ArgumentCaptor<Values> emmitedValues;

    private Tuple tuple = EsTestUtil.generateTestTuple(source, index, type, documentId);

    @Override
    protected EsLookupBolt createBolt(EsConfig esConfig) {
        return new EsLookupBolt(esConfig);
    }

    @BeforeEach
    public void populateIndexWithTestData() throws Exception {
        node.client().prepareIndex(index, type, documentId).setSource(source).execute().actionGet();
    }

    @Test
    public void anchorsTheTuple() throws Exception {
        bolt.execute(tuple);

        verify(outputCollector).emit(anchor.capture(), emmitedValues.capture());
        assertThat(anchor.getValue(), is(tuple));
    }

    @Test
    public void emitsExpectedValues() throws Exception {
        Values expectedValues = expectedValues();

        bolt.execute(tuple);

        verify(outputCollector).emit(anchor.capture(), emmitedValues.capture());
        assertThat(emmitedValues.getValue(), is(expectedValues));
    }

    @Test
    public void acksTuple() throws Exception {
        bolt.execute(tuple);

        verify(outputCollector).ack(anchor.capture());
        assertThat(anchor.getValue(), is(tuple));
    }

    @Test
    public void indexMissing() throws Exception {
        Tuple tuple = EsTestUtil.generateTestTuple(source, "missing", type, documentId);
        bolt.execute(tuple);

        verify(outputCollector, never()).emit(any(Tuple.class), any(Values.class));
        verify(outputCollector).reportError(any(ResponseException.class));
        verify(outputCollector).fail(tuple);
    }

    private Values expectedValues() {
        return new Values(index, type, documentId, source);
    }

    @Override
    protected Class<EsLookupBolt> getBoltClass() {
        return EsLookupBolt.class;
    }
}
