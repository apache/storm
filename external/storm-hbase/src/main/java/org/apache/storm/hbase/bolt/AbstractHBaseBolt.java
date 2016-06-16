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
package org.apache.storm.hbase.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractHBaseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseBolt.class);

    protected OutputCollector collector;

    protected transient HBaseClient hBaseClient;
    protected String tableName;
    protected HBaseMapper mapper;
    protected String configKey;

    private static final String DEFAULT_ZK_HOSTS = "localhost";
    private static final String DEFAULT_ZK_PORT = "2181";
    private static final String DEFAULT_ZK_PARENT = "/hbase";

    private String zkHosts = DEFAULT_ZK_HOSTS;
    private String zkPort = DEFAULT_ZK_PORT;
    private String zkParent = DEFAULT_ZK_PARENT;

    public AbstractHBaseBolt(String tableName, HBaseMapper mapper) {
        Validate.notEmpty(tableName, "Table name can not be blank or null");
        Validate.notNull(mapper, "mapper can not be null");
        this.tableName = tableName;
        this.mapper = mapper;
    }

    public void setZkHosts(String hosts) {
        this.zkHosts = hosts;
    }

    public void setZkPort(String port) {
        this.zkPort = port;
    }

    public void setZkParent(String parent) {
        this.zkParent = parent;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        final Configuration hbConfig = HBaseConfiguration.create();

        Map<String, Object> conf = (Map<String, Object>)map.get(this.configKey);
        if(conf == null) {
            throw new IllegalArgumentException("HBase configuration not found using key '" + this.configKey + "'");
        }

        if(conf.get("hbase.rootdir") == null) {
            LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }
        for(String key : conf.keySet()) {
            hbConfig.set(key, String.valueOf(conf.get(key)));
        }

        hbConfig.set("hbase.zookeeper.quorum", zkHosts);
        hbConfig.set("hbase.zookeeper.property.clientPort", zkPort);
        hbConfig.set("zookeeper.znode.parent", zkParent);

        //heck for backward compatibility, we need to pass TOPOLOGY_AUTO_CREDENTIALS to hbase conf
        //the conf instance is instance of persistentMap so making a copy.
        Map<String, Object> hbaseConfMap = new HashMap<String, Object>(conf);
        hbaseConfMap.put(Config.TOPOLOGY_AUTO_CREDENTIALS, map.get(Config.TOPOLOGY_AUTO_CREDENTIALS));
        this.hBaseClient = new HBaseClient(hbaseConfMap, hbConfig, tableName);
    }
}
