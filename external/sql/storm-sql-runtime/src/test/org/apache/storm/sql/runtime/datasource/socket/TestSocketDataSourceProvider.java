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

package org.apache.storm.sql.runtime.datasource.socket;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.sql.runtime.datasource.socket.trident.SocketState;
import org.apache.storm.sql.runtime.datasource.socket.trident.SocketStateUpdater;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSocketDataSourceProvider {
    private static final List<FieldInfo> FIELDS = ImmutableList.of(
            new FieldInfo("ID", int.class, true),
            new FieldInfo("val", String.class, false));
    private static final List<String> FIELD_NAMES = ImmutableList.of("ID", "val");
    private static final JsonSerializer SERIALIZER = new JsonSerializer(FIELD_NAMES);

    @Test
    public void testSocketSink() throws IOException {
        ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
                URI.create("socket://localhost:8888"), null, null, new Properties(), FIELDS);
        Assert.assertNotNull(ds);

        ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

        Assert.assertEquals(SocketState.Factory.class, consumer.getStateFactory().getClass());
        Assert.assertEquals(SocketStateUpdater.class, consumer.getStateUpdater().getClass());

        // makeState() fails on creating State so we just mock SocketState anyway
        SocketState mockState = mock(SocketState.class);
        StateUpdater stateUpdater = consumer.getStateUpdater();

        List<TridentTuple> tupleList = mockTupleList();

        stateUpdater.updateState(mockState, tupleList, null);
        for (TridentTuple t : tupleList) {
            String serializedValue = new String(SERIALIZER.write(t.getValues(), null).array());
            verify(mockState).write(serializedValue + "\n");
        }
    }

    private static List<TridentTuple> mockTupleList() {
        List<TridentTuple> tupleList = new ArrayList<>();
        TridentTuple t0 = mock(TridentTuple.class);
        TridentTuple t1 = mock(TridentTuple.class);
        when(t0.getValueByField("ID")).thenReturn(1);
        when(t0.getValueByField("val")).thenReturn("2");
        doReturn(Lists.<Object>newArrayList(1, "2")).when(t0).getValues();
        when(t0.size()).thenReturn(2);

        when(t1.getValueByField("ID")).thenReturn(2);
        when(t1.getValueByField("val")).thenReturn("3");
        doReturn(Lists.<Object>newArrayList(2, "3")).when(t1).getValues();
        when(t1.size()).thenReturn(2);

        tupleList.add(t0);
        tupleList.add(t1);
        return tupleList;
    }
}