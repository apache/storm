/*
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

package org.apache.storm.iceberg.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class IcebergWriterBoltTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private String warehouse;
    private HadoopCatalog verifyCatalog;
    private OutputCollector collector;

    @BeforeEach
    void setUp() {
        warehouse = tempDir.toUri().toString();
        verifyCatalog = new HadoopCatalog(new Configuration(), warehouse);
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        collector = mock(OutputCollector.class);
    }

    @AfterEach
    void tearDown() throws IOException {
        verifyCatalog.close();
    }

    private IcebergOptions.Builder baseOptions() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        return new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("db.events");
    }

    private IcebergWriterBolt prepared(IcebergOptions options) {
        IcebergWriterBolt bolt = new IcebergWriterBolt(options);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "test-topology");
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(7);
        when(context.getThisComponentId()).thenReturn("iceberg-writer");
        when(context.getThisTaskIndex()).thenReturn(0);
        bolt.prepare(topoConf, context, collector);
        return bolt;
    }

    private Tuple tuple(long id, String name) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("spout");
        when(tuple.contains(anyString())).thenReturn(false);
        when(tuple.contains("id")).thenReturn(true);
        when(tuple.contains("name")).thenReturn(true);
        when(tuple.getValueByField("id")).thenReturn(id);
        when(tuple.getValueByField("name")).thenReturn(name);
        return tuple;
    }

    private Tuple tickTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        return tuple;
    }

    @Test
    void nothingIsEmittedOrAckedBeforeTheSealThreshold() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(3).build());

        bolt.execute(tuple(1L, "alice"));
        bolt.execute(tuple(2L, "bob"));

        verify(collector, never()).emit(anyCollection(), anyList());
        verify(collector, never()).ack(any());
        bolt.cleanup();
    }

    @Test
    void sealingEmitsOneDescriptorAnchoredToEveryTupleOfTheBatch() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(2).build());
        Tuple first = tuple(1L, "alice");
        Tuple second = tuple(2L, "bob");

        bolt.execute(first);
        bolt.execute(second);

        ArgumentCaptor<Collection<Tuple>> anchors = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<List<Object>> values = ArgumentCaptor.forClass(List.class);
        verify(collector, times(1)).emit(anchors.capture(), values.capture());
        assertEquals(List.of(first, second), List.copyOf(anchors.getValue()),
            "the descriptor is a child of every tuple in the batch");
        assertEquals(7, values.getValue().get(0), "the writer task id travels with the descriptor");
        assertTrue(values.getValue().get(1).toString().contains("file_path")
                || values.getValue().get(1).toString().contains("file-path"),
            "the descriptor carries serialized data files");
        verify(collector, times(2)).ack(any());
        // The order is the guarantee: acking before emitting the anchored descriptor would close
        // the spout's ack tree while the commit is still outstanding.
        InOrder inOrder = inOrder(collector);
        inOrder.verify(collector).emit(anyCollection(), anyList());
        inOrder.verify(collector, times(2)).ack(any());
        bolt.cleanup();
    }

    @Test
    void aTickTupleSealsWhatIsBuffered() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());

        bolt.execute(tuple(1L, "alice"));
        bolt.execute(tickTuple());

        verify(collector, times(1)).emit(anyCollection(), anyList());
        verify(collector, times(1)).ack(any());
        bolt.cleanup();
    }

    @Test
    void aTickTupleWithNothingBufferedEmitsNothing() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());

        bolt.execute(tickTuple());

        verify(collector, never()).emit(anyCollection(), anyList());
        verify(collector, never()).ack(any());
        bolt.cleanup();
    }

    @Test
    void aWriteFailureFailsTheOpenBatchAndEmitsNothing() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(5).build());
        bolt.execute(tuple(1L, "alice"));
        Tuple wrongType = mock(Tuple.class);
        when(wrongType.getSourceComponent()).thenReturn("spout");
        when(wrongType.contains(anyString())).thenReturn(false);
        when(wrongType.contains("id")).thenReturn(true);
        when(wrongType.contains("name")).thenReturn(true);
        when(wrongType.getValueByField("id")).thenReturn("not-a-long");
        when(wrongType.getValueByField("name")).thenReturn("carol");

        bolt.execute(wrongType);

        verify(collector, never()).emit(anyCollection(), anyList());
        verify(collector, never()).ack(any());
        verify(collector, times(2)).fail(any());
    }

    @Test
    void cleanupFailsWhatWasNeverSealed() {
        IcebergWriterBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());
        bolt.execute(tuple(1L, "alice"));

        bolt.cleanup();

        verify(collector, never()).ack(any());
        verify(collector, times(1)).fail(any());
    }
}
