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

package org.apache.storm.utils;

import java.util.Map;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this class only to login to HDFS from storm servers.
 */
public class HdfsLoginUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsLoginUtil.class);

    private static HdfsLoginUtil singleton;

    private IHdfsLoginPlugin hdfsLoginPlugin;

    private HdfsLoginUtil() {
        // private constructor
    }

    /**
     * Get the singleton instance.
     * @return the singleton instance
     */
    public static HdfsLoginUtil getInstance() {
        if (singleton == null) {
            synchronized (HdfsLoginUtil.class) {
                if (singleton == null) {
                    singleton = new HdfsLoginUtil();
                }
            }
        }
        return singleton;
    }

    /**
     * Login to hdfs.
     * We use the single HdfsLoginPlugin instance because it may use some local state/cache.
     * @param conf the storm configuration
     * @return the logged in subject
     */
    public Subject logintoHdfs(Map<String, Object> conf) {

        if (hdfsLoginPlugin == null) {
            synchronized (HdfsLoginUtil.class) {
                if (hdfsLoginPlugin == null) {
                    String className = (String) conf.get(Config.STORM_HDFS_LOGIN_PLUGIN);
                    if (className != null) {
                        LOG.info("The hdfs login plugin to use is {}", className);
                        hdfsLoginPlugin = ReflectionUtils.newInstance(className);
                    } else {
                        throw new IllegalArgumentException(Config.STORM_HDFS_LOGIN_PLUGIN + " is null or not set");
                    }
                }
            }
        }

        return hdfsLoginPlugin.login(conf);
    }
}
