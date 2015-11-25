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
package com.alibaba.jstorm.ui.utils;

import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.TimeCacheMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class NimbusClientManager {
    private static Logger LOG = LoggerFactory.getLogger(NimbusClientManager.class);

    protected static TimeCacheMap<String, NimbusClient> clientManager;

    static {
        clientManager = new TimeCacheMap<String, NimbusClient>(3600,
                new ExpiredCallback<String, NimbusClient>() {

                    @Override
                    public void expire(String key, NimbusClient val) {
                        LOG.info("Close connection of " + key);
                        val.close();
                    }

                });
    }

    public static NimbusClient getNimbusClient(String clusterName) throws Exception {
        Map conf = UIUtils.readUiConfig();
        NimbusClient client = clientManager.get(clusterName);
        if (client != null) {
            try {
                client.getClient().getVersion();
                LOG.info("get Nimbus Client from clientManager");
            } catch (Exception e) {
                LOG.info("Nimbus has been restarted, it begin to reconnect");
                client = null;
            }
        }

        if (client == null) {
            conf = UIUtils.resetZKConfig(conf, clusterName);
            client = NimbusClient.getConfiguredClient(conf);
            clientManager.put(clusterName, client);
        }

        return client;
    }

    public static void removeClient(String clusterName) {
        if (StringUtils.isBlank(clusterName)) {
            clusterName = UIDef.DEFAULT_CLUSTER_NAME;
        }
        NimbusClient client = (NimbusClient) clientManager.remove(clusterName);
        if (client != null) {
            client.close();
        }
    }

    public static int getClientSize(){
        return clientManager.size();
    }

}