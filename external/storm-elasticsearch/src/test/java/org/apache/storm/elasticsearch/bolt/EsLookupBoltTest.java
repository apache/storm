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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.elasticsearch.EsLookupResultOutput;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class EsLookupBoltTest extends AbstractEsBoltTest<EsLookupBolt> {

    static final Map<String, String> params = new HashMap<>();

    @Mock
    private EsTupleMapper tupleMapper;

    @Mock
    private EsLookupResultOutput output;

    @Mock
    private Tuple tuple;

    @Mock
    private RestClient client;

    private RestClient originalClient;

    @Override
    protected EsLookupBolt createBolt(EsConfig esConfig) {
        originalClient = EsLookupBolt.getClient();
        EsLookupBolt.replaceClient(this.client);
        return new EsLookupBolt(esConfig, tupleMapper, output);
    }

    @AfterEach
    public void replaceClientWithOriginal() {
        EsLookupBolt.replaceClient(originalClient);
    }

    @BeforeEach
    public void configureBoltDependencies() {
        when(tupleMapper.getIndex(tuple)).thenReturn(index);
        when(tupleMapper.getType(tuple)).thenReturn(type);
        when(tupleMapper.getId(tuple)).thenReturn(documentId);
    }

    private void makeRequestAndThrow(Exception exception) throws IOException {
        when(client.performRequest("get", AbstractEsBolt.getEndpoint(index, type, documentId), params)).thenThrow(exception);
        bolt.execute(tuple);
    }

    @Test
    public void failsTupleWhenClientThrows() throws Exception {
        makeRequestAndThrow(EsTestUtil.generateMockResponseException());
        verify(outputCollector).fail(tuple);
    }

    @Test
    public void reportsExceptionWhenClientThrows() throws Exception {
        ResponseException responseException = EsTestUtil.generateMockResponseException();
        makeRequestAndThrow(responseException);

        verify(outputCollector).reportError(responseException);
    }

    @Test
    public void fieldsAreDeclaredThroughProvidedOutput() {
        Fields fields = new Fields(UUID.randomUUID().toString());
        when(output.fields()).thenReturn(fields);
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        bolt.declareOutputFields(declarer);

        ArgumentCaptor<Fields> declaredFields = ArgumentCaptor.forClass(Fields.class);
        verify(declarer).declare(declaredFields.capture());

        assertThat(declaredFields.getValue(), is(fields));
    }

    @Override
    protected Class<EsLookupBolt> getBoltClass() {
        return EsLookupBolt.class;
    }
}
