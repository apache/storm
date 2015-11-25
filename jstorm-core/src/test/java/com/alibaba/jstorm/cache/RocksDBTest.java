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
package com.alibaba.jstorm.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.OSInfo;
import com.alibaba.jstorm.utils.RunCounter;

public class RocksDBTest {
    private static Logger LOG = LoggerFactory.getLogger(RocksDBTest.class);

    public static String rootDir = "rocksdb_test";
    // ATTENTIME
    // DUE to windows can't load RocksDB jni libray,
    // So these junit tests can't run under window
    //

    RocksTTLDBCache cache = new RocksTTLDBCache();
    {
        Map conf = new HashMap();
        // TemporaryFolder cacheRoot = new TemporaryFolder()

        conf.put(RocksDBCache.ROCKSDB_ROOT_DIR, rootDir);
        conf.put(RocksDBCache.ROCKSDB_RESET, true);
        List<Integer> list = new ArrayList<Integer>() {
            {
                add(60);
                add(120);
            }
        };
        conf.put(RocksDBCache.TAG_TIMEOUT_LIST, list);

        try {
            cache.init(conf);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void testSimple() {
        List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 4; i++) {
            dataList.add(new HashMap<String, Object>());
        }

        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            for (int j = 0; j < 4; j++) {
                Map<String, Object> data = dataList.get(j);
                String value = "value--" + i + "--" + j;

                data.put(key, value);
            }
        }

        Map<String, Object> data = dataList.get(0);
        for (Entry<String, Object> entry : data.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Object> entry : data.entrySet()) {
            String rawValue = (String) entry.getValue();
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(rawValue, fetchValue);
        }

        for (Entry<String, Object> entry : data.entrySet()) {
            cache.remove(entry.getKey());
        }

        for (Entry<String, Object> entry : data.entrySet()) {
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(fetchValue, null);
        }

        Map<String, Object> data1 = dataList.get(1);
        cache.putBatch(data1);

        Map<String, Object> getBatch = new HashMap<String, Object>();

        for (Entry<String, Object> entry : data1.entrySet()) {
            getBatch.put(entry.getKey(), null);
        }
        Map<String, Object> emptyBatch = new HashMap<String, Object>();
        emptyBatch.putAll(getBatch);
        cache.getBatch(getBatch);

        for (String key : getBatch.keySet()) {
            String rawValue = (String) data1.get(key);
            String fetchValue = (String) getBatch.get(key);

            Assert.assertEquals(rawValue, fetchValue);
        }

        cache.removeBatch(data1.keySet());

        getBatch.clear();
        getBatch.putAll(emptyBatch);
        cache.getBatch(getBatch);

        for (String key : getBatch.keySet()) {
            String rawValue = null;
            String fetchValue = (String) getBatch.get(key);

            Assert.assertEquals(rawValue, fetchValue);
        }

        Map<String, Object> data2 = dataList.get(2);
        for (Entry<String, Object> entry : data2.entrySet()) {
            cache.put(entry.getKey(), entry.getValue(), 60);
        }

        for (Entry<String, Object> entry : data2.entrySet()) {
            String rawValue = (String) entry.getValue();
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(rawValue, fetchValue);
        }

        for (Entry<String, Object> entry : data2.entrySet()) {
            cache.remove(entry.getKey());
        }

        for (Entry<String, Object> entry : data2.entrySet()) {
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(fetchValue, null);
        }

        Map<String, Object> data3 = dataList.get(3);
        cache.putBatch(data3, 60);

        for (Entry<String, Object> entry : data3.entrySet()) {
            getBatch.put(entry.getKey(), null);
        }

        cache.getBatch(getBatch);

        for (String key : getBatch.keySet()) {
            String rawValue = (String) data3.get(key);
            String fetchValue = (String) getBatch.get(key);

            Assert.assertEquals(rawValue, fetchValue);
        }

        cache.removeBatch(data3.keySet());

        getBatch.clear();
        getBatch.putAll(emptyBatch);
        cache.getBatch(getBatch);

        for (String key : getBatch.keySet()) {
            String rawValue = null;
            String fetchValue = (String) getBatch.get(key);

            Assert.assertEquals(rawValue, fetchValue);
        }

        for (Entry<String, Object> entry : data.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }
        cache.putBatch(data3, 60);

        for (Entry<String, Object> entry : data3.entrySet()) {
            String rawValue = (String) entry.getValue();
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(rawValue, fetchValue);
        }

        cache.removeBatch(data3.keySet());

        for (Entry<String, Object> entry : data3.entrySet()) {
            String rawValue = null;
            String fetchValue = (String) cache.get(entry.getKey());

            Assert.assertEquals(rawValue, fetchValue);
        }

        // double delete
        cache.removeBatch(data3.keySet());
    }

    public void visitorAccross() throws RocksDBException, InterruptedException {
        DBOptions dbOptions = null;
        TtlDB ttlDB = null;
        List<ColumnFamilyDescriptor> cfNames = new ArrayList<ColumnFamilyDescriptor>();
        List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();
        cfNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        cfNames.add(new ColumnFamilyDescriptor("new_cf".getBytes()));

        List<Integer> ttlValues = new ArrayList<Integer>();
        // new column family with 1 second ttl
        ttlValues.add(1);

        // Default column family with infinite lifetime
        ttlValues.add(0);

        try {
            System.out.println("Begin to open db");
            dbOptions = new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
            ttlDB = TtlDB.open(dbOptions, rootDir, cfNames, columnFamilyHandleList, ttlValues, false);

            System.out.println("Successfully open db " + rootDir);

            List<String> keys = new ArrayList<String>();
            keys.add("key");
            ttlDB.put("key".getBytes(), "key".getBytes());
            for (int i = 0; i < 2; i++) {
                String key = "key" + i;
                keys.add(key);

                ttlDB.put(columnFamilyHandleList.get(i), key.getBytes(), key.getBytes());
            }

            try {
                byte[] value = ttlDB.get("others".getBytes());
                if (value != null) {
                    System.out.println("Raw get :" + new String(value));
                } else {
                    System.out.println("No value of other");
                }
            } catch (Exception e) {
                System.out.println("Occur exception other");
            }

            for (String key : keys) {
                try {
                    byte[] value = ttlDB.get(key.getBytes());
                    if (value != null) {
                        System.out.println("Raw get :" + new String(value));
                    } else {
                        System.out.println("No value of " + key);
                    }
                } catch (Exception e) {
                    System.out.println("Occur exception " + key + ", Raw");
                }

                for (int i = 0; i < 2; i++) {
                    try {
                        byte[] value = ttlDB.get(columnFamilyHandleList.get(i), key.getBytes());
                        if (value != null) {
                            System.out.println("handler index" + i + " get :" + new String(value));
                        } else {
                            System.out.println("No value of index" + i + " get :" + key);
                        }
                    } catch (Exception e) {
                        System.out.println("Occur exception " + key + ", handler index:" + i);
                    }

                }
            }

        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.dispose();
            }
            if (ttlDB != null) {
                ttlDB.close();
            }
            if (dbOptions != null) {
                dbOptions.dispose();
            }
        }
    }

    public void ttlDbOpenWithColumnFamilies() throws RocksDBException, InterruptedException {
        DBOptions dbOptions = null;
        TtlDB ttlDB = null;
        List<ColumnFamilyDescriptor> cfNames = new ArrayList<ColumnFamilyDescriptor>();
        List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();
        cfNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        cfNames.add(new ColumnFamilyDescriptor("new_cf".getBytes()));

        List<Integer> ttlValues = new ArrayList<Integer>();
        // Default column family with infinite lifetime
        ttlValues.add(0);
        // new column family with 1 second ttl
        ttlValues.add(1);

        try {
            System.out.println("Begin to open db");
            dbOptions = new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
            ttlDB = TtlDB.open(dbOptions, rootDir, cfNames, columnFamilyHandleList, ttlValues, false);

            System.out.println("Successfully open db " + rootDir);

            ttlDB.put("key".getBytes(), "value".getBytes());
            assertThat(ttlDB.get("key".getBytes())).isEqualTo("value".getBytes());
            ttlDB.put(columnFamilyHandleList.get(1), "key".getBytes(), "value".getBytes());
            assertThat(ttlDB.get(columnFamilyHandleList.get(1), "key".getBytes())).isEqualTo("value".getBytes());
            TimeUnit.SECONDS.sleep(2);

            ttlDB.compactRange();
            ttlDB.compactRange(columnFamilyHandleList.get(1));

            assertThat(ttlDB.get("key".getBytes())).isNotNull();
            assertThat(ttlDB.get(columnFamilyHandleList.get(1), "key".getBytes())).isNull();

        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.dispose();
            }
            if (ttlDB != null) {
                ttlDB.close();
            }
            if (dbOptions != null) {
                dbOptions.dispose();
            }
        }
    }

    public void performanceTest() {
        final LinkedBlockingDeque<String> getQueue = new LinkedBlockingDeque<String>();
        final LinkedBlockingDeque<String> rmQueue = new LinkedBlockingDeque<String>();

        final int TIMES = 10000000;

        Thread putThread = new Thread(new Runnable() {
            RunCounter runCounter = new RunCounter("Put");

            @Override
            public void run() {
                // TODO Auto-generated method stub
                for (int i = 0; i < TIMES; i++) {
                    String key = "Key" + i;

                    cache.put(key, key);

                    runCounter.count(1l);

                    getQueue.offer(key);
                }

                getQueue.offer(null);

                System.out.println("Shutdown ");
            }
        });

        Thread getThread = new Thread(new Runnable() {
            RunCounter runCounter = new RunCounter("Get");

            @Override
            public void run() {
                // TODO Auto-generated method stub
                while (true) {
                    String key = null;
                    try {
                        key = getQueue.take();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    if (key == null) {
                        break;
                    }

                    cache.get(key);

                    runCounter.count(1l);

                    rmQueue.offer(key);
                }
                System.out.println("Shutdown get");
            }
        });

        Thread rmThread = new Thread(new Runnable() {
            RunCounter runCounter = new RunCounter("Rm");

            @Override
            public void run() {
                // TODO Auto-generated method stub
                while (true) {
                    String key = null;
                    try {
                        key = rmQueue.take();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    if (key == null) {
                        break;
                    }

                    cache.remove(key);

                    runCounter.count(1l);

                }
                System.out.println("Shutdown rm");
            }
        });

        System.out.println("Start performance test");
        putThread.start();
        getThread.start();
        rmThread.start();

        JStormUtils.sleepMs(1000000);
    }

    public static void main(String[] args) throws Exception {
        if (OSInfo.isLinux() || OSInfo.isMac()) {
            RocksDB.loadLibrary();

            RocksDBTest instance = new RocksDBTest();

            // instance.testSimple();
            // instance.ttlDbOpenWithColumnFamilies();
            // instance.visitorAccross();

            instance.performanceTest();
        }

    }

}
