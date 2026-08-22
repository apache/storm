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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
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

class IcebergSplitSinkTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "region", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private String warehouse;
    private HadoopCatalog verifyCatalog;
    private OutputCollector writerCollector;
    private OutputCollector committerCollector;
    private IcebergCommitterBolt committer;

    @BeforeEach
    void setUp() {
        warehouse = tempDir.toUri().toString();
        verifyCatalog = new HadoopCatalog(new Configuration(), warehouse);
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        writerCollector = mock(OutputCollector.class);
        committerCollector = mock(OutputCollector.class);
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

    /** Route whatever the writer emits straight into the committer, as the topology would. */
    private void wireWriterIntoCommitter() {
        doAnswer(invocation -> {
            List<Object> values = invocation.getArgument(1);
            Tuple descriptor = mock(Tuple.class);
            when(descriptor.getSourceComponent()).thenReturn("iceberg-writer");
            when(descriptor.getIntegerByField(IcebergWriterBolt.FIELD_WRITER_TASK_ID))
                .thenReturn((Integer) values.get(0));
            when(descriptor.getStringByField(IcebergWriterBolt.FIELD_DATA_FILES))
                .thenReturn((String) values.get(1));
            committer.execute(descriptor);
            return List.of();
        }).when(writerCollector).emit(anyCollection(), anyList());
    }

    private IcebergWriterBolt preparedWriter(IcebergOptions options, int taskId) {
        IcebergWriterBolt bolt = new IcebergWriterBolt(options);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "test-topology");
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(taskId);
        when(context.getThisComponentId()).thenReturn("iceberg-writer");
        when(context.getThisTaskIndex()).thenReturn(taskId);
        bolt.prepare(topoConf, context, writerCollector);
        return bolt;
    }

    private IcebergCommitterBolt preparedCommitter(IcebergOptions options) {
        IcebergCommitterBolt bolt = new IcebergCommitterBolt(options);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "test-topology");
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(99);
        when(context.getThisComponentId()).thenReturn("iceberg-committer");
        when(context.getThisTaskIndex()).thenReturn(0);
        when(context.getComponentTasks("iceberg-committer")).thenReturn(List.of(99));
        bolt.prepare(topoConf, context, committerCollector);
        return bolt;
    }

    private Tuple tuple(long id, String region) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("spout");
        when(tuple.contains(anyString())).thenReturn(false);
        when(tuple.contains("id")).thenReturn(true);
        when(tuple.contains("region")).thenReturn(true);
        when(tuple.getValueByField("id")).thenReturn(id);
        when(tuple.getValueByField("region")).thenReturn(region);
        return tuple;
    }

    private Tuple tickTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        return tuple;
    }

    private List<Record> readRows() {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        table.refresh();
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            records.forEach(rows::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    private int snapshotCount() {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        table.refresh();
        int count = 0;
        for (Object ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    @Test
    void everyRowFromEveryWriterEndsUpInOneSnapshot() {
        IcebergOptions options = baseOptions()
            .withCommitIntervalRecords(2)
            .withGroupCommitMaxDataFiles(3)
            .withGroupCommitIntervalMillis(600_000L)
            .build();
        committer = preparedCommitter(options);
        wireWriterIntoCommitter();
        IcebergWriterBolt first = preparedWriter(options, 1);
        IcebergWriterBolt second = preparedWriter(options, 2);
        IcebergWriterBolt third = preparedWriter(options, 3);

        first.execute(tuple(1L, "eu-west"));
        first.execute(tuple(2L, "eu-west"));
        second.execute(tuple(3L, "us-east"));
        second.execute(tuple(4L, "us-east"));
        third.execute(tuple(5L, "eu-west"));
        third.execute(tuple(6L, "eu-west"));

        assertEquals(6, readRows().size(), "every row is visible");
        assertEquals(1, snapshotCount(), "three writers produced one snapshot, not three");
        verify(writerCollector, times(6)).ack(any());
        verify(committerCollector, times(3)).ack(any());
        first.cleanup();
        second.cleanup();
        third.cleanup();
        committer.cleanup();
    }

    @Test
    void nothingIsVisibleUntilTheCommitterCommits() {
        IcebergOptions options = baseOptions()
            .withCommitIntervalRecords(1)
            .withGroupCommitMaxDataFiles(100)
            .withGroupCommitIntervalMillis(600_000L)
            .build();
        committer = preparedCommitter(options);
        wireWriterIntoCommitter();
        IcebergWriterBolt writer = preparedWriter(options, 1);

        writer.execute(tuple(1L, "eu-west"));

        assertEquals(0, readRows().size(), "the writer sealed but the committer has not committed");
        verify(committerCollector, times(0)).ack(any());

        committer.execute(tickTuple());

        assertEquals(1, readRows().size());
        verify(committerCollector, times(1)).ack(any());
        writer.cleanup();
        committer.cleanup();
    }

    @Test
    void filesSealedAgainstDifferentSpecsCommitTogether() {
        IcebergOptions options = baseOptions()
            .withCommitIntervalRecords(1)
            .withGroupCommitMaxDataFiles(100)
            .withGroupCommitIntervalMillis(600_000L)
            .build();
        committer = preparedCommitter(options);
        wireWriterIntoCommitter();
        IcebergWriterBolt writer = preparedWriter(options, 1);

        writer.execute(tuple(1L, "eu-west"));
        // The spec evolves between two seals, so the group spans two spec ids.
        verifyCatalog.loadTable(TABLE_ID).updateSpec().addField("region").commit();
        writer.execute(tuple(2L, "us-east"));
        committer.execute(tickTuple());

        assertEquals(2, readRows().size(), "both spec generations are visible");
        assertEquals(1, snapshotCount());
        writer.cleanup();
        committer.cleanup();
    }
}
