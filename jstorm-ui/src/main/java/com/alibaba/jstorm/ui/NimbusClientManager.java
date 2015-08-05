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
package com.alibaba.jstorm.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.TimeCacheMap;

public class NimbusClientManager {
    private static Logger LOG = LoggerFactory
            .getLogger(NimbusClientManager.class);

    public static String DEFAULT = "default";

    protected static TimeCacheMap<String, NimbusClient> clientManager;
    static {

        clientManager =
                new TimeCacheMap<String, NimbusClient>(3600,
                        new ExpiredCallback<String, NimbusClient>() {

                            @Override
                            public void expire(String key, NimbusClient val) {
                                // TODO Auto-generated method stub
                                LOG.info("Close connection of " + key);
                                val.close();
                            }

                        });
    }

    public static NimbusClient getNimbusClient(String clusterName)
            throws Exception {

        Map conf = UIUtils.readUiConfig();

        if (DEFAULT.equals(clusterName)) {
            // do nothing
        } else if (StringUtils.isBlank(clusterName) == false) {
            UIUtils.getClusterInfoByName(conf, clusterName);
        }
        return NimbusClient.getConfiguredClient(conf);
    }

    public static NimbusClient getNimbusClient(Map<String, String> parameterMap)
            throws Exception {

        String clusterName = parameterMap.get(UIDef.CLUSTER);
        if (StringUtils.isBlank(clusterName) == true) {
            clusterName = DEFAULT;
        }

        NimbusClient client = clientManager.get(clusterName);
        if (client != null) {
        	try {
        		client.getClient().getVersion();
        	}catch(Exception e) {
        		LOG.info("Nimbus has been restarted, it begin to reconnect");
        		client = null;
        	}
        }
        
        if (client != null) {
            return client;
        }

        client = getNimbusClient(clusterName);

        clientManager.put(clusterName, client);

        return client;

    }

    public static void removeClient(Map<String, String> parameterMap) {
        String clusterName = parameterMap.get(UIDef.CLUSTER);
        if (StringUtils.isBlank(clusterName) == true) {
            clusterName = DEFAULT;
        }
        NimbusClient client =  (NimbusClient)clientManager.remove(clusterName);
        if (client != null) {
            client.close();
        }
    }
}
