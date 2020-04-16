/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.multitenant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.utils.ConfigLoaderFactoryService;
import org.apache.storm.scheduler.utils.IConfigLoader;
import org.apache.storm.scheduler.utils.SchedulerConfigCache;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultitenantScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(MultitenantScheduler.class);
    protected IConfigLoader configLoader;
    private Map<String, Object> conf;
    private SchedulerConfigCache<Map<String, Number>> schedulerConfigCache;

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        this.conf = conf;
        configLoader = ConfigLoaderFactoryService.createConfigLoader(conf);
        schedulerConfigCache = new SchedulerConfigCache<>(conf, this::loadConfig);
        schedulerConfigCache.prepare();
    }

    /**
     * Load from configLoaders first; if no config available, read from multitenant-scheduler.yaml;
     * if no config available from multitenant-scheduler.yaml, get configs from conf. Only one will be used.
     * @return User pool configs.
     */
    private Map<String, Number> loadConfig() {
        Map<String, Number> ret;

        // Try the loader plugin, if configured
        if (configLoader != null) {
            ret = (Map<String, Number>) configLoader.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            if (ret != null) {
                return ret;
            } else {
                LOG.warn("Config loader returned null. Will try to read from multitenant-scheduler.yaml");
            }
        }

        // If that fails, fall back on the multitenant-scheduler.yaml file
        Map fromFile = Utils.findAndReadConfigFile("multitenant-scheduler.yaml", false);
        ret = (Map<String, Number>) fromFile.get(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        if (ret != null) {
            return ret;
        } else {
            LOG.warn("Reading from multitenant-scheduler.yaml returned null. This could because the file is not available. "
                     + "Will load configs from storm configuration");
        }

        // If that fails, use config
        ret = (Map<String, Number>) conf.get(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        if (ret == null) {
            return Collections.emptyMap();
        } else {
            return ret;
        }
    }

    @Override
    public Map<String, Number> config() {
        return Collections.unmodifiableMap(schedulerConfigCache.get());
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("Rerunning scheduling...");
        //refresh the config every time before scheduling
        schedulerConfigCache.refresh();

        Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster);

        Map<String, Number> userConf = config();

        Map<String, IsolatedPool> userPools = new HashMap<>();
        for (Map.Entry<String, Number> entry : userConf.entrySet()) {
            userPools.put(entry.getKey(), new IsolatedPool(entry.getValue().intValue()));
        }
        DefaultPool defaultPool = new DefaultPool();
        FreePool freePool = new FreePool();

        freePool.init(cluster, nodeIdToNode);
        for (IsolatedPool pool : userPools.values()) {
            pool.init(cluster, nodeIdToNode);
        }
        defaultPool.init(cluster, nodeIdToNode);

        for (TopologyDetails td : topologies.getTopologies()) {
            String user = td.getTopologySubmitter();
            LOG.debug("Found top {} run by user {}", td.getId(), user);
            NodePool pool = userPools.get(user);
            if (pool == null || !pool.canAdd(td)) {
                pool = defaultPool;
            }
            pool.addTopology(td);
        }

        //Now schedule all of the topologies that need to be scheduled
        for (IsolatedPool pool : userPools.values()) {
            pool.scheduleAsNeeded(freePool, defaultPool);
        }
        defaultPool.scheduleAsNeeded(freePool);
        LOG.debug("Scheduling done...");
    }
}
