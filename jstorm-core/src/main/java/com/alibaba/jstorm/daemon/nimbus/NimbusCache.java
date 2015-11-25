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
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.cache.RocksDBCache;
import com.alibaba.jstorm.cache.TimeoutMemCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.OSInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NimbusCache {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusCache.class);

    public static final String TIMEOUT_MEM_CACHE_CLASS = TimeoutMemCache.class.getName();
    public static final String ROCKS_DB_CACHE_CLASS = RocksDBCache.class.getName();

    protected JStormCache memCache;
    protected JStormCache dbCache;
    protected StormClusterState zkCluster;

    public String getNimbusCacheClass(Map conf) {
        boolean isLinux = OSInfo.isLinux();
        boolean isMac = OSInfo.isMac();
        boolean isLocal = StormConfig.local_mode(conf);

        if (isLocal == true) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        if (isLinux == false && isMac == false) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        String nimbusCacheClass = ConfigExtension.getNimbusCacheClass(conf);
        if (StringUtils.isBlank(nimbusCacheClass) == false) {
            return nimbusCacheClass;
        }

        return ROCKS_DB_CACHE_CLASS;

    }

    public NimbusCache(Map conf, StormClusterState zkCluster) {
        super();

        String dbCacheClass = getNimbusCacheClass(conf);
        LOG.info("NimbusCache db Cache will use {}", dbCacheClass);

        try {
            dbCache = (JStormCache) Utils.newInstance(dbCacheClass);

            String dbDir = StormConfig.masterDbDir(conf);
            conf.put(RocksDBCache.ROCKSDB_ROOT_DIR, dbDir);

            conf.put(RocksDBCache.ROCKSDB_RESET, ConfigExtension.getNimbusCacheReset(conf));

            dbCache.init(conf);

            if (dbCache instanceof TimeoutMemCache) {
                memCache = dbCache;
            } else {
                memCache = new TimeoutMemCache();
                memCache.init(conf);
            }
        } catch (UnsupportedClassVersionError e) {

            if (e.getMessage().indexOf("Unsupported major.minor version") >= 0) {
                LOG.error("!!!Please update jdk version to 7 or higher!!!");

            }
            LOG.error("Failed to create NimbusCache!", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Failed to create NimbusCache!", e);
            throw new RuntimeException(e);
        }

        this.zkCluster = zkCluster;
    }

    public JStormCache getMemCache() {
        return memCache;
    }

    public JStormCache getDbCache() {
        return dbCache;
    }

    public void cleanup() {
        dbCache.cleanup();

    }

    /**
     * 
     * In the old design, DBCache will cache all taskInfo/taskErrors, this will be useful for huge topology
     * 
     * But the latest zk design, taskInfo is only one znode, taskErros has few znode So remove them from DBCache Skip timely refresh taskInfo/taskErrors
     * 
     */

}
