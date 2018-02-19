/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hbasemetricstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.FilterOptions;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseStoreTest {
    private static HBaseStore store = new TestableHBaseStore();
    private static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();

    private static class TestableHBaseStore extends HBaseStore {
        @Override
        protected Configuration createConfiguration(Map mapConfig) throws MetricException {
            Configuration conf = super.createConfiguration(mapConfig);
            int zkPort = hbaseTestingUtility.getZkCluster().getClientPort();
            conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
            return conf;
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        hbaseTestingUtility.startMiniCluster();
        createTable();

        Map<String, Object> conf = new HashMap<>();
        store.prepare(conf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseTestingUtility.shutdownMiniCluster();
    }

    private static void createTable() throws IOException {
        HBaseAdmin admin = hbaseTestingUtility.getHBaseAdmin();
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("metrics"));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("metricdata").setMaxVersions(HConstants.ALL_VERSIONS);
        descriptor.addFamily(columnDescriptor);
        columnDescriptor = new HColumnDescriptor("metadata").setMaxVersions(HConstants.ALL_VERSIONS);
        descriptor.addFamily(columnDescriptor);
        admin.createTable(descriptor);
    }

    @Test
    public void testAggregation() throws Exception {
        double sum0 = 0.0;
        double sum1 = 0.0;
        double sum10 = 0.0;
        double sum60 = 0.0;
        Metric toPopulate = null;
        for (int i=0; i<20; i++) {
            double value = 5 + i;
            long timestamp = 1L + i*60*1000;
            Metric m = new Metric("cpu", timestamp, "myTopologyId123", value,
                    "componentId1", "executorId1", "hostname1", "streamid1", 7777, AggLevel.AGG_LEVEL_NONE);
            toPopulate = new Metric(m);
            store.insert(m);

            if (timestamp < 60*1000) {
                sum0 += value;
                sum1 += value;
                sum10 += value;
                sum60 += value;
            } else if (timestamp < 600*1000) {
                sum10 += value;
                sum60 += value;
            } else {
                sum60 += value;
            }
        }

        waitForInsertFinish(toPopulate);

        toPopulate.setTimestamp(1L);
        toPopulate.setAggLevel(AggLevel.AGG_LEVEL_NONE);
        boolean res = store.populateValue(toPopulate);
        Assert.assertEquals(true, res);
        Assert.assertEquals(sum0, toPopulate.getSum(), 0.001);
        Assert.assertEquals(sum0, toPopulate.getValue(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMin(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMax(), 0.001);
        Assert.assertEquals(1, toPopulate.getCount());

        toPopulate.setTimestamp(0L);
        toPopulate.setAggLevel(AggLevel.AGG_LEVEL_1_MIN);
        res = store.populateValue(toPopulate);
        Assert.assertEquals(true, res);
        Assert.assertEquals(sum1, toPopulate.getSum(), 0.001);
        Assert.assertEquals(sum1, toPopulate.getValue(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMin(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMax(), 0.001);
        Assert.assertEquals(1, toPopulate.getCount());

        toPopulate.setTimestamp(0L);
        toPopulate.setAggLevel(AggLevel.AGG_LEVEL_10_MIN);
        res = store.populateValue(toPopulate);
        Assert.assertEquals(true, res);
        Assert.assertEquals(sum10, toPopulate.getSum(), 0.001);
        Assert.assertEquals(sum10/10.0, toPopulate.getValue(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMin(), 0.001);
        Assert.assertEquals(14.0, toPopulate.getMax(), 0.001);
        Assert.assertEquals(10, toPopulate.getCount());

        toPopulate.setTimestamp(0L);
        toPopulate.setAggLevel(AggLevel.AGG_LEVEL_60_MIN);
        res = store.populateValue(toPopulate);
        Assert.assertEquals(true, res);
        Assert.assertEquals(sum60, toPopulate.getSum(), 0.001);
        Assert.assertEquals(sum60/20.0, toPopulate.getValue(), 0.001);
        Assert.assertEquals(5.0, toPopulate.getMin(), 0.001);
        Assert.assertEquals(24.0, toPopulate.getMax(), 0.001);
        Assert.assertEquals(20, toPopulate.getCount());
    }

    @Test
    public void testPopulateFailure() throws Exception {
        Metric m = new Metric("cpu", 3000L, "myTopologyId456", 1.0,
                "componentId2", "executorId2", "hostname2", "streamid2", 7778, AggLevel.AGG_LEVEL_NONE);
        store.insert(m);
        waitForInsertFinish(m);
        Metric toFind = new Metric(m);
        toFind.setTopologyId("somethingBogus");
        boolean res = store.populateValue(toFind);
        Assert.assertEquals(false, res);
    }

    private List<Metric> getMetricsFromScan(FilterOptions filter) throws MetricException {
        List<Metric> list = new ArrayList<>();
        store.scan(filter, (Metric m) -> {
            list.add(m);
        });
        return list;
    }

    @Test
    public void testScan() throws Exception {
        FilterOptions filter;
        List<Metric> list;

        Metric m1 = new Metric("metricType1", 50000000L, "Topo-m1", 1.0,
                "component-1", "executor-2", "hostname-1", "stream-1", 1, AggLevel.AGG_LEVEL_NONE);
        Metric m2 = new Metric("metricType2", 50030000L, "Topo-m1", 1.0,
                "component-1", "executor-1", "hostname-2", "stream-2", 1, AggLevel.AGG_LEVEL_NONE);
        Metric m3 = new Metric("metricType3", 50200000L, "Topo-m1", 1.0,
                "component-2", "executor-1", "hostname-1", "stream-3", 1, AggLevel.AGG_LEVEL_NONE);
        Metric m4 = new Metric("metricType4", 50200000L, "Topo-m2", 1.0,
                "component-2", "executor-1", "hostname-2", "stream-4", 2, AggLevel.AGG_LEVEL_NONE);
        store.insert(m1);
        store.insert(m2);
        store.insert(m3);
        store.insert(m4);
        waitForInsertFinish(m4);

        // validate search by time
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setStartTime(50000000L);
        filter.setEndTime(50130000L);
        list = getMetricsFromScan(filter);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(m1));
        Assert.assertTrue(list.contains(m2));

        // validate search by topology id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setTopologyId("Topo-m2");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.contains(m4));

        // validate search by metric id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setMetricName("metricType2");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.contains(m2));

        // validate search by component id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setComponentId("component-2");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(m3));
        Assert.assertTrue(list.contains(m4));

        // validate search by executor id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setExecutorId("executor-1");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(list.contains(m2));
        Assert.assertTrue(list.contains(m3));
        Assert.assertTrue(list.contains(m4));

        // validate search by executor id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setExecutorId("executor-1");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(list.contains(m2));
        Assert.assertTrue(list.contains(m3));
        Assert.assertTrue(list.contains(m4));

        // validate search by host id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setHostId("hostname-2");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(m2));
        Assert.assertTrue(list.contains(m4));

        // validate search by port
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setPort(1);
        list = getMetricsFromScan(filter);
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(list.contains(m1));
        Assert.assertTrue(list.contains(m2));
        Assert.assertTrue(list.contains(m3));

        // validate search by stream id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setStreamId("stream-4");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(list.contains(m4));

        // validate 4 metrics (aggregations) found for m4 for all agglevels when searching by port
        filter = new FilterOptions();
        filter.setPort(2);
        list = getMetricsFromScan(filter);
        Assert.assertEquals(4, list.size());
        Assert.assertTrue(list.contains(m4));
        Assert.assertFalse(list.contains(m1));
        Assert.assertFalse(list.contains(m2));
        Assert.assertFalse(list.contains(m3));

        // validate search by topology id and executor id
        filter = new FilterOptions();
        filter.addAggLevel(AggLevel.AGG_LEVEL_NONE);
        filter.setTopologyId("Topo-m1");
        filter.setExecutorId("executor-1");
        list = getMetricsFromScan(filter);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(m2));
        Assert.assertTrue(list.contains(m3));
    }

    private void waitForInsertFinish(Metric m) throws Exception {
        Metric last = new Metric(m);
        int attempts = 0;
        do {
            Thread.sleep(1);
            attempts++;
            if (attempts > 5000) {
                throw new Exception("Insertion timing out");
            }
        } while (!store.populateValue(last));

        // add some delay to allow aggregation to finish
        Thread.sleep(30);
    }
}
