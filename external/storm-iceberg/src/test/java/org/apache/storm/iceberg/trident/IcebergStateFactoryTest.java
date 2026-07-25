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

package org.apache.storm.iceberg.trident;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergStateFactoryTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    @Test
    void makeStatePreparesAndUpdaterWritesBatch() throws IOException {
        String warehouse = tempDir.toUri().toString();
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
            catalog.createTable(TABLE_ID, SCHEMA);

            Map<String, String> catalogProps = new HashMap<>();
            catalogProps.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
            catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
            IcebergOptions options = new IcebergOptions.Builder()
                .withCatalogProperties(catalogProps)
                .withTable("db.events")
                .build();

            Map<String, Object> conf = new HashMap<>();
            conf.put(Config.TOPOLOGY_NAME, "factory-test");

            State state = new IcebergStateFactory(options).makeState(conf, null, 0, 1);
            IcebergState icebergState = assertInstanceOf(IcebergState.class, state);
            try {
                TridentTuple tuple = mock(TridentTuple.class);
                when(tuple.contains(anyString())).thenReturn(false);
                when(tuple.contains("id")).thenReturn(true);
                when(tuple.contains("name")).thenReturn(true);
                when(tuple.getValueByField("id")).thenReturn(99L);
                when(tuple.getValueByField("name")).thenReturn("via-factory");

                icebergState.beginCommit(1L);
                new IcebergStateUpdater().updateState(icebergState, List.of(tuple), null);
                icebergState.commit(1L);

                List<Record> rows = new ArrayList<>();
                try (CloseableIterable<Record> iterable = IcebergGenerics.read(catalog.loadTable(TABLE_ID)).build()) {
                    iterable.forEach(rows::add);
                }
                assertEquals(1, rows.size());
                assertEquals(99L, rows.get(0).getField("id"));
            } finally {
                icebergState.close();
            }
        }
    }
}
