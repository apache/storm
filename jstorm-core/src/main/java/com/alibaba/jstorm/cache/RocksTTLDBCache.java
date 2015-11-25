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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

public class RocksTTLDBCache implements JStormCache {
    private static final long serialVersionUID = 705938812240167583L;
    private static Logger LOG = LoggerFactory.getLogger(RocksTTLDBCache.class);

    static {
        RocksDB.loadLibrary();
    }

    public static final String ROCKSDB_ROOT_DIR = "rocksdb.root.dir";
    public static final String ROCKSDB_RESET = "rocksdb.reset";
    protected TtlDB ttlDB;
    protected String rootDir;
    protected TreeMap<Integer, ColumnFamilyHandle> windowHandlers = new TreeMap<Integer, ColumnFamilyHandle>();

    public void initDir(Map<Object, Object> conf) {
        String confDir = (String) conf.get(ROCKSDB_ROOT_DIR);
        if (StringUtils.isBlank(confDir) == true) {
            throw new RuntimeException("Doesn't set rootDir of rocksDB");
        }

        boolean clean = ConfigExtension.getNimbusCacheReset(conf);
        LOG.info("RocksDB reset is " + clean);
        if (clean == true) {
            try {
                PathUtils.rmr(confDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException("Failed to cleanup rooDir of rocksDB " + confDir);
            }
        }

        File file = new File(confDir);
        if (file.exists() == false) {
            try {
                PathUtils.local_mkdirs(confDir);
                file = new File(confDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException("Failed to mkdir rooDir of rocksDB " + confDir);
            }
        }

        rootDir = file.getAbsolutePath();
    }

    public void initDb(List<Integer> list) throws Exception {
        LOG.info("Begin to init rocksDB of {}", rootDir);

        DBOptions dbOptions = null;

        List<ColumnFamilyDescriptor> columnFamilyNames = new ArrayList<ColumnFamilyDescriptor>();
        columnFamilyNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (Integer timeout : list) {
            columnFamilyNames.add(new ColumnFamilyDescriptor(String.valueOf(timeout).getBytes()));
        }

        List<Integer> ttlValues = new ArrayList<Integer>();
        // Default column family with infinite lifetime
        // ATTENSION, the first must be 0, RocksDB.java API has this limitation
        ttlValues.add(0);
        // new column family with list second ttl
        ttlValues.addAll(list);

        try {
            dbOptions = new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);

            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();

            ttlDB = TtlDB.open(dbOptions, rootDir, columnFamilyNames, columnFamilyHandleList, ttlValues, false);

            for (int i = 0; i < ttlValues.size(); i++) {
                windowHandlers.put(ttlValues.get(i), columnFamilyHandleList.get(i));
            }

            LOG.info("Successfully init rocksDB of {}", rootDir);
        } finally {

            if (dbOptions != null) {
                dbOptions.dispose();
            }
        }
    }

    @Override
    public void init(Map<Object, Object> conf) throws Exception {
        // TODO Auto-generated method stub
        initDir(conf);

        List<Integer> list = new ArrayList<Integer>();
        if (conf.get(TAG_TIMEOUT_LIST) != null) {
            for (Object obj : (List) ConfigExtension.getCacheTimeoutList(conf)) {
                Integer timeoutSecond = JStormUtils.parseInt(obj);
                if (timeoutSecond == null || timeoutSecond <= 0) {
                    continue;
                }

                list.add(timeoutSecond);
            }
        }

        // Add retry logic
        boolean isSuccess = false;
        for (int i = 0; i < 3; i++) {
            try {
                initDb(list);
                isSuccess = true;
                break;
            } catch (Exception e) {
                LOG.warn("Failed to init rocksDB " + rootDir, e);
                try {
                    PathUtils.rmr(rootDir);
                } catch (IOException e1) {
                    // TODO Auto-generated catch block

                }
            }
        }

        if (isSuccess == false) {
            throw new RuntimeException("Failed to init rocksDB " + rootDir);
        }
    }

    @Override
    public void cleanup() {
        LOG.info("Begin to close rocketDb of {}", rootDir);

        for (ColumnFamilyHandle columnFamilyHandle : windowHandlers.values()) {
            columnFamilyHandle.dispose();
        }

        if (ttlDB != null) {
            ttlDB.close();
        }

        LOG.info("Successfully closed rocketDb of {}", rootDir);
    }

    @Override
    public Object get(String key) {
        // TODO Auto-generated method stub
        for (Entry<Integer, ColumnFamilyHandle> entry : windowHandlers.entrySet()) {
            try {
                byte[] data = ttlDB.get(entry.getValue(), key.getBytes());
                if (data != null) {
                    try {
                        return Utils.javaDeserialize(data);
                    } catch (Exception e) {
                        LOG.error("Failed to deserialize obj of " + key);
                        ttlDB.remove(entry.getValue(), key.getBytes());
                        return null;
                    }
                }

            } catch (Exception e) {

            }
        }

        return null;
    }

    @Override
    public void getBatch(Map<String, Object> map) {
        List<byte[]> lookupKeys = new ArrayList<byte[]>();
        for (String key : map.keySet()) {
            lookupKeys.add(key.getBytes());
        }
        for (Entry<Integer, ColumnFamilyHandle> entry : windowHandlers.entrySet()) {

            List<ColumnFamilyHandle> cfHandlers = new ArrayList<ColumnFamilyHandle>();
            for (String key : map.keySet()) {
                cfHandlers.add(entry.getValue());
            }

            try {
                Map<byte[], byte[]> results = ttlDB.multiGet(cfHandlers, lookupKeys);
                if (results == null || results.size() == 0) {
                    continue;
                }

                for (Entry<byte[], byte[]> resultEntry : results.entrySet()) {
                    byte[] keyByte = resultEntry.getKey();
                    byte[] valueByte = resultEntry.getValue();

                    if (keyByte == null || valueByte == null) {
                        continue;
                    }

                    Object value = null;
                    try {
                        value = Utils.javaDeserialize(valueByte);
                    } catch (Exception e) {
                        LOG.error("Failed to deserialize obj of " + new String(keyByte));
                        ttlDB.remove(entry.getValue(), keyByte);
                        continue;
                    }

                    map.put(new String(keyByte), value);
                }

                return;
            } catch (Exception e) {
                LOG.error("Failed to query " + map.keySet() + ", in window: " + entry.getKey());
            }
        }

        return;
    }

    @Override
    public void remove(String key) {
        for (Entry<Integer, ColumnFamilyHandle> entry : windowHandlers.entrySet()) {
            try {
                ttlDB.remove(entry.getValue(), key.getBytes());

            } catch (Exception e) {
                LOG.error("Failed to remove " + key);
            }
        }
    }

    @Override
    public void removeBatch(Collection<String> keys) {
        // TODO Auto-generated method stub
        for (String key : keys) {
            remove(key);
        }
    }

    protected void put(String key, Object value, Entry<Integer, ColumnFamilyHandle> entry) {

        byte[] data = Utils.javaSerialize(value);
        try {
            ttlDB.put(entry.getValue(), key.getBytes(), data);
        } catch (Exception e) {
            LOG.error("Failed put into cache, " + key, e);
            return;
        }

        for (Entry<Integer, ColumnFamilyHandle> removeEntry : windowHandlers.entrySet()) {
            if (removeEntry.getKey().equals(entry.getKey())) {
                continue;
            }

            try {
                ttlDB.remove(removeEntry.getValue(), key.getBytes());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.warn("Failed to remove other " + key);
            }
        }
    }

    protected Entry<Integer, ColumnFamilyHandle> getHandler(int timeoutSecond) {
        ColumnFamilyHandle cfHandler = null;
        Entry<Integer, ColumnFamilyHandle> ceilingEntry = windowHandlers.ceilingEntry(timeoutSecond);
        if (ceilingEntry != null) {
            return ceilingEntry;
        } else {
            return windowHandlers.firstEntry();
        }
    }

    @Override
    public void put(String key, Object value, int timeoutSecond) {
        // TODO Auto-generated method stub

        put(key, value, getHandler(timeoutSecond));

    }

    @Override
    public void put(String key, Object value) {
        put(key, value, windowHandlers.firstEntry());
    }

    protected void putBatch(Map<String, Object> map, Entry<Integer, ColumnFamilyHandle> putEntry) {
        // TODO Auto-generated method stub
        WriteOptions writeOpts = null;
        WriteBatch writeBatch = null;

        Set<byte[]> putKeys = new HashSet<byte[]>();

        try {
            writeOpts = new WriteOptions();
            writeBatch = new WriteBatch();

            for (Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                byte[] data = Utils.javaSerialize(value);

                if (StringUtils.isBlank(key) || data == null || data.length == 0) {
                    continue;
                }

                byte[] keyByte = key.getBytes();
                writeBatch.put(putEntry.getValue(), keyByte, data);

                putKeys.add(keyByte);
            }

            ttlDB.write(writeOpts, writeBatch);
        } catch (Exception e) {
            LOG.error("Failed to putBatch into DB, " + map.keySet(), e);
        } finally {
            if (writeOpts != null) {
                writeOpts.dispose();
            }

            if (writeBatch != null) {
                writeBatch.dispose();
            }
        }

        for (Entry<Integer, ColumnFamilyHandle> entry : windowHandlers.entrySet()) {
            if (entry.getKey().equals(putEntry.getKey())) {
                continue;
            }
            for (byte[] keyByte : putKeys) {
                try {
                    ttlDB.remove(entry.getValue(), keyByte);

                } catch (Exception e) {
                    LOG.error("Failed to remove other's " + new String(keyByte));
                }
            }
        }
    }

    @Override
    public void putBatch(Map<String, Object> map) {
        // TODO Auto-generated method stub
        putBatch(map, windowHandlers.firstEntry());
    }

    @Override
    public void putBatch(Map<String, Object> map, int timeoutSeconds) {
        // TODO Auto-generated method stub
        putBatch(map, getHandler(timeoutSeconds));
    }

    // public void put() throws Exception {

    // }
    //
    // public void write() throws Exception {
    // Options options = null;
    // WriteBatch wb1 = null;
    // WriteBatch wb2 = null;
    // WriteOptions opts = null;
    // try {
    // options = new Options().setMergeOperator(new StringAppendOperator()).setCreateIfMissing(true);
    // db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
    // opts = new WriteOptions();
    // wb1 = new WriteBatch();
    // wb1.put("key1".getBytes(), "aa".getBytes());
    // wb1.merge("key1".getBytes(), "bb".getBytes());
    // wb2 = new WriteBatch();
    // wb2.put("key2".getBytes(), "xx".getBytes());
    // wb2.merge("key2".getBytes(), "yy".getBytes());
    // db.write(opts, wb1);
    // db.write(opts, wb2);
    // assertThat(db.get("key1".getBytes())).isEqualTo("aa,bb".getBytes());
    // assertThat(db.get("key2".getBytes())).isEqualTo("xx,yy".getBytes());
    // } finally {
    // if (db != null) {
    // db.close();
    // }
    // if (wb1 != null) {
    // wb1.dispose();
    // }
    // if (wb2 != null) {
    // wb2.dispose();
    // }
    // if (options != null) {
    // options.dispose();
    // }
    // if (opts != null) {
    // opts.dispose();
    // }
    // }
    // }
    //

    //
    // public void remove() throws Exception {
    // RocksDB db = null;
    // WriteOptions wOpt;
    // try {
    // wOpt = new WriteOptions();
    // db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
    // db.put("key1".getBytes(), "value".getBytes());
    // db.put("key2".getBytes(), "12345678".getBytes());
    // assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
    // assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());
    // db.remove("key1".getBytes());
    // db.remove(wOpt, "key2".getBytes());
    // assertThat(db.get("key1".getBytes())).isNull();
    // assertThat(db.get("key2".getBytes())).isNull();
    // } finally {
    // if (db != null) {
    // db.close();
    // }
    // }
    // }
    //
    // public void ttlDbOpenWithColumnFamilies() throws Exception, InterruptedException {
    //
    // }
}
