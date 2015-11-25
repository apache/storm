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

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import org.apache.commons.lang.StringUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class RocksDBCache implements JStormCache {
    private static final long serialVersionUID = 705938812240167583L;
    private static Logger LOG = LoggerFactory.getLogger(RocksDBCache.class);

    static {
        RocksDB.loadLibrary();
    }

    public static final String ROCKSDB_ROOT_DIR = "rocksdb.root.dir";
    public static final String ROCKSDB_RESET = "rocksdb.reset";
    protected RocksDB db;
    protected String rootDir;

    public void initDir(Map<Object, Object> conf) {
        String confDir = (String) conf.get(ROCKSDB_ROOT_DIR);
        if (StringUtils.isBlank(confDir) == true) {
            throw new RuntimeException("Doesn't set rootDir of rocksDB");
        }

        boolean clean = (Boolean) conf.get(ROCKSDB_RESET);
        LOG.info("RocksDB reset is " + clean);
        if (clean == true) {
            try {
                PathUtils.rmr(confDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to cleanup rooDir of rocksDB " + confDir);
            }
        }

        File file = new File(confDir);
        if (file.exists() == false) {
            try {
                PathUtils.local_mkdirs(confDir);
                file = new File(confDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to mkdir rooDir of rocksDB " + confDir);
            }
        }

        rootDir = file.getAbsolutePath();
    }

    public void initDb(List<Integer> list) throws Exception {
        LOG.info("Begin to init rocksDB of {}", rootDir);

        Options dbOptions = null;
        try {
            dbOptions = new Options().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);

            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();

            db = RocksDB.open(dbOptions, rootDir);

            LOG.info("Successfully init rocksDB of {}", rootDir);
        } finally {
            if (dbOptions != null) {
                dbOptions.dispose();
            }
        }
    }

    @Override
    public void init(Map<Object, Object> conf) throws Exception {
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
                } catch (IOException ignored) {
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

        if (db != null) {
            db.close();
        }

        LOG.info("Successfully closed rocketDb of {}", rootDir);
    }

    @Override
    public Object get(String key) {
        try {
            byte[] data = db.get(key.getBytes());
            if (data != null) {
                try {
                    return Utils.javaDeserialize(data);
                } catch (Exception e) {
                    LOG.error("Failed to deserialize obj of " + key);
                    db.remove(key.getBytes());
                    return null;
                }
            }

        } catch (Exception e) {
        }

        return null;
    }

    @Override
    public void getBatch(Map<String, Object> map) {
        List<byte[]> lookupKeys = new ArrayList<byte[]>();
        for (String key : map.keySet()) {
            lookupKeys.add(key.getBytes());
        }

        try {
            Map<byte[], byte[]> results = db.multiGet(lookupKeys);
            if (results == null || results.size() == 0) {
                return;
            }

            for (Entry<byte[], byte[]> resultEntry : results.entrySet()) {
                byte[] keyByte = resultEntry.getKey();
                byte[] valueByte = resultEntry.getValue();

                if (keyByte == null || valueByte == null) {
                    continue;
                }

                Object value;
                try {
                    value = Utils.javaDeserialize(valueByte);
                } catch (Exception e) {
                    LOG.error("Failed to deserialize obj of " + new String(keyByte));
                    db.remove(keyByte);
                    continue;
                }

                map.put(new String(keyByte), value);
            }
        } catch (Exception e) {
            LOG.error("Failed to query " + map.keySet() + ", in window: ");
        }
    }

    @Override
    public void remove(String key) {
        try {
            db.remove(key.getBytes());

        } catch (Exception e) {
            LOG.error("Failed to remove " + key);
        }

    }

    @Override
    public void removeBatch(Collection<String> keys) {
        for (String key : keys) {
            remove(key);
        }
    }

    @Override
    public void put(String key, Object value, int timeoutSecond) {
        put(key, value);
    }

    @Override
    public void put(String key, Object value) {
        byte[] data = Utils.javaSerialize(value);
        try {
            db.put(key.getBytes(), data);
        } catch (Exception e) {
            LOG.error("Failed put into cache, " + key, e);
        }
    }

    @Override
    public void putBatch(Map<String, Object> map) {
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
                writeBatch.put(keyByte, data);

                putKeys.add(keyByte);
            }

            db.write(writeOpts, writeBatch);
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

    }

    @Override
    public void putBatch(Map<String, Object> map, int timeoutSeconds) {
        putBatch(map);
    }
}
