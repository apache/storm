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

package org.apache.storm.hbase.state;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class HBaseClientTestUtil {
    private HBaseClientTestUtil() {
    }

    public static HBaseClient mockedHBaseClient() throws Exception {
        return mockedHBaseClient(new ConcurrentSkipListMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>>(
            UnsignedBytes.lexicographicalComparator()));
    }

    public static HBaseClient mockedHBaseClient(
        ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> internalMap)
        throws Exception {
        HBaseClient mockClient = mock(HBaseClient.class);

        Mockito.doNothing().when(mockClient).close();

        Mockito.when(mockClient.constructGetRequests(any(byte[].class), any(HBaseProjectionCriteria.class)))
               .thenCallRealMethod();

        Mockito.when(mockClient.constructMutationReq(any(byte[].class), any(ColumnList.class), any(Durability.class)))
               .thenCallRealMethod();

        Mockito.when(mockClient.exists(any(Get.class))).thenAnswer(new ExistsAnswer(internalMap));
        Mockito.when(mockClient.batchGet(any(List.class))).thenAnswer(new BatchGetAnswer(internalMap));
        Mockito.doAnswer(new BatchMutateAnswer(internalMap)).when(mockClient).batchMutate(any(List.class));
        Mockito.when(mockClient.scan(any(byte[].class), any(byte[].class))).thenAnswer(new ScanAnswer(internalMap));

        return mockClient;
    }

    static class BuildCellsHelper {
        public static void addMatchingColumnFamilies(byte[] rowKey, Map<byte[], NavigableSet<byte[]>> familyMap,
                                                     NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValueMap,
                                                     List<Cell> cells) {
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                byte[] columnFamily = entry.getKey();

                NavigableMap<byte[], byte[]> qualifierToValueMap = cfToQualifierToValueMap.get(columnFamily);
                if (qualifierToValueMap != null) {
                    if (entry.getValue() == null || entry.getValue().size() == 0) {
                        addAllQualifiers(rowKey, columnFamily, qualifierToValueMap, cells);
                    } else {
                        addMatchingQualifiers(rowKey, columnFamily, entry, qualifierToValueMap, cells);
                    }
                }
            }
        }

        public static void addMatchingQualifiers(byte[] rowKey, byte[] columnFamily,
                                                 Map.Entry<byte[], NavigableSet<byte[]>> qualifierSet,
                                                 NavigableMap<byte[], byte[]> qualifierToValueMap,
                                                 List<Cell> cells) {
            for (byte[] qualifier : qualifierSet.getValue()) {
                byte[] value = qualifierToValueMap.get(qualifier);
                if (value != null) {
                    cells.add(new KeyValue(rowKey, columnFamily, qualifier, value));
                }
            }
        }

        public static void addAllColumnFamilies(byte[] rowKey, NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValueMap,
                                                List<Cell> cells) {
            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : cfToQualifierToValueMap.entrySet()) {
                byte[] columnFamily = entry.getKey();
                addAllQualifiers(rowKey, columnFamily, entry.getValue(), cells);
            }
        }

        public static void addAllQualifiers(byte[] rowKey, byte[] columnFamily,
                                            NavigableMap<byte[], byte[]> qualifierToValueMap, List<Cell> cells) {
            for (Map.Entry<byte[], byte[]> entry2 : qualifierToValueMap.entrySet()) {
                byte[] qualifier = entry2.getKey();
                byte[] value = entry2.getValue();
                cells.add(new KeyValue(rowKey, columnFamily, qualifier, value));
            }
        }

    }

    static class BatchGetAnswer implements Answer<Result[]> {
        private final ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap;

        public BatchGetAnswer(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap) {
            this.mockMap = mockMap;
        }

        @Override
        public Result[] answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object[] args = invocationOnMock.getArguments();
            List<Get> param = (List<Get>) args[0];

            List<Result> results = new ArrayList<>(param.size());

            for (Get get : param) {
                byte[] rowKey = get.getRow();

                NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValueMap =
                    mockMap.get(rowKey);

                if (cfToQualifierToValueMap != null) {
                    Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();

                    List<Cell> cells = new ArrayList<>();
                    if (familyMap == null || familyMap.size() == 0) {
                        // all column families
                        BuildCellsHelper.addAllColumnFamilies(rowKey, cfToQualifierToValueMap, cells);
                    } else {
                        // one or more column families
                        BuildCellsHelper.addMatchingColumnFamilies(rowKey, familyMap, cfToQualifierToValueMap, cells);
                    }

                    // Result.create() states that "You must ensure that the keyvalues are already sorted."
                    Collections.sort(cells, new KeyValue.KVComparator());
                    results.add(Result.create(cells));
                } else {
                    results.add(Result.EMPTY_RESULT);
                }
            }

            return results.toArray(new Result[0]);
        }
    }

    static class BatchMutateAnswer implements Answer {
        private final ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap;

        public BatchMutateAnswer(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap) {
            this.mockMap = mockMap;
        }

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object[] args = invocationOnMock.getArguments();
            List<Mutation> param = (List<Mutation>) args[0];

            // assumption: there're no put and delete for same target in parameter list
            for (Mutation mutation : param) {
                byte[] rowKey = mutation.getRow();

                NavigableMap<byte[], List<Cell>> familyCellMap = mutation.getFamilyCellMap();
                if (familyCellMap == null || familyCellMap.size() == 0) {
                    if (mutation instanceof Delete) {
                        deleteRow(mockMap, rowKey);
                    } else {
                        throw new IllegalStateException("Not supported in mocked mutate.");
                    }
                }

                for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                    byte[] columnFamily = entry.getKey();
                    List<Cell> cells = entry.getValue();

                    if (cells == null || cells.size() == 0) {
                        if (mutation instanceof Delete) {
                            deleteColumnFamily(mockMap, rowKey, columnFamily);
                        } else {
                            throw new IllegalStateException("Not supported in mocked mutate.");
                        }
                    } else {
                        for (Cell cell : cells) {
                            byte[] qualifier = CellUtil.cloneQualifier(cell);

                            if (mutation instanceof Put) {
                                byte[] value = CellUtil.cloneValue(cell);

                                putCell(mockMap, rowKey, columnFamily, qualifier, value);
                            } else if (mutation instanceof Delete) {
                                deleteCell(mockMap, rowKey, columnFamily, qualifier);
                            } else {
                                throw new IllegalStateException("Not supported in mocked mutate.");
                            }
                        }
                    }
                }
            }

            return null;
        }

        private void putCell(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap,
                             byte[] rowKey, byte[] columnFamily, byte[] qualifier, byte[] value) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValue = mockMap.get(rowKey);
            if (cfToQualifierToValue == null) {
                cfToQualifierToValue = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                mockMap.put(rowKey, cfToQualifierToValue);
            }

            NavigableMap<byte[], byte[]> qualifierToValue = cfToQualifierToValue.get(columnFamily);
            if (qualifierToValue == null) {
                qualifierToValue = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                cfToQualifierToValue.put(columnFamily, qualifierToValue);
            }

            qualifierToValue.put(qualifier, value);
        }

        private void deleteRow(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap,
                               byte[] rowKey) {
            mockMap.remove(rowKey);
        }

        private void deleteColumnFamily(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap,
                                        byte[] rowKey, byte[] columnFamily) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValue = mockMap.get(rowKey);
            if (cfToQualifierToValue != null) {
                cfToQualifierToValue.remove(columnFamily);
            }
        }

        private void deleteCell(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap,
                                byte[] rowKey, byte[] columnFamily, byte[] qualifier) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValue = mockMap.get(rowKey);
            if (cfToQualifierToValue != null) {
                NavigableMap<byte[], byte[]> qualifierToValue = cfToQualifierToValue.get(columnFamily);
                if (qualifierToValue != null) {
                    qualifierToValue.remove(qualifier);
                }
            }
        }
    }

    static class ExistsAnswer implements Answer<Boolean> {
        private final ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap;

        public ExistsAnswer(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap) {
            this.mockMap = mockMap;
        }

        @Override
        public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object[] args = invocationOnMock.getArguments();
            Get param = (Get) args[0];

            // assume that Get doesn't have any families defined. this is for not digging deeply...
            byte[] rowKey = param.getRow();
            Map<byte[], NavigableSet<byte[]>> familyMap = param.getFamilyMap();
            if (familyMap.size() > 0) {
                throw new IllegalStateException("Not supported in mocked exists.");
            }

            return mockMap.containsKey(rowKey);
        }
    }

    static class ScanAnswer implements Answer<ResultScanner> {
        private final ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> mockMap;

        public ScanAnswer(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> internalMap) {
            this.mockMap = internalMap;
        }

        @Override
        public ResultScanner answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object[] args = invocationOnMock.getArguments();
            byte[] startKey = (byte[]) args[0];
            byte[] endKey = (byte[]) args[1];

            final ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> subMap =
                mockMap.subMap(startKey, true, endKey, false);

            final List<Result> results = buildResults(subMap);

            return new MockedResultScanner(results);
        }

        private List<Result> buildResults(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> subMap) {
            final List<Result> results = new ArrayList<>();
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<byte[], byte[]>>> entry : subMap.entrySet()) {
                byte[] rowKey = entry.getKey();
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfToQualifierToValueMap = entry.getValue();
                List<Cell> cells = new ArrayList<>();
                // all column families
                BuildCellsHelper.addAllColumnFamilies(rowKey, cfToQualifierToValueMap, cells);

                // Result.create() states that "You must ensure that the keyvalues are already sorted."
                Collections.sort(cells, new KeyValue.KVComparator());
                results.add(Result.create(cells));
            }
            return results;
        }

        static class MockedResultScanner implements ResultScanner {

            private final List<Result> results;
            private int position = 0;

            MockedResultScanner(List<Result> results) {
                this.results = results;
            }

            @Override
            public Result next() {
                if (results.size() <= position) {
                    return null;
                }
                return results.get(position++);
            }

            @Override
            public Result[] next(int nbRows) {
                List<Result> bulkResult = new ArrayList<>();
                for (int i = 0; i < nbRows; i++) {
                    Result result = next();
                    if (result == null) {
                        break;
                    }

                    bulkResult.add(result);
                }
                return bulkResult.toArray(new Result[0]);
            }

            @Override
            public void close() {
                //NO-OP
            }

            @Override
            public boolean renewLease() {
                return true;
            }

            @Override
            public ScanMetrics getScanMetrics() {
                return null;
            }

            @Override
            public Iterator<Result> iterator() {
                return results.iterator();
            }
        }
    }
}
