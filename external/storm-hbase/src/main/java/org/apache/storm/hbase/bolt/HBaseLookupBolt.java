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

package org.apache.storm.hbase.bolt;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt for querying from HBase.
 *
 * <p>Note: Each HBaseBolt defined in a topology is tied to a specific table.
 */
public class HBaseLookupBolt extends AbstractHBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupBolt.class);

    private HBaseValueMapper rowToTupleMapper;
    private HBaseProjectionCriteria projectionCriteria;
    private transient LoadingCache<byte[], Result> cache;
    private transient boolean cacheEnabled;

    public HBaseLookupBolt(String tableName, HBaseMapper mapper, HBaseValueMapper rowToTupleMapper) {
        super(tableName, mapper);
        Validate.notNull(rowToTupleMapper, "rowToTupleMapper can not be null");
        this.rowToTupleMapper = rowToTupleMapper;
    }

    public HBaseLookupBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public HBaseLookupBolt withProjectionCriteria(HBaseProjectionCriteria projectionCriteria) {
        this.projectionCriteria = projectionCriteria;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> config, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(config, topologyContext, collector);
        cacheEnabled = Boolean.parseBoolean(config.getOrDefault("hbase.cache.enable", "false").toString());
        int cacheTtl = Integer.parseInt(config.getOrDefault("hbase.cache.ttl.seconds", "300").toString());
        int maxCacheSize = Integer.parseInt(config.getOrDefault("hbase.cache.size", "1000").toString());
        if (cacheEnabled) {
            cache = Caffeine.newBuilder().maximumSize(maxCacheSize).expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
                            .build(new CacheLoader<byte[], Result>() {

                                @Override
                                public Result load(byte[] rowKey) throws Exception {
                                    Get get = hBaseClient.constructGetRequests(rowKey, projectionCriteria);
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Cache miss for key:" + new String(rowKey));
                                    }
                                    return hBaseClient.batchGet(Lists.newArrayList(get))[0];
                                }

                            });
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            collector.ack(tuple);
            return;
        }
        byte[] rowKey = this.mapper.rowKey(tuple);
        Result result = null;
        try {
            if (cacheEnabled) {
                result = cache.get(rowKey);
            } else {
                Get get = hBaseClient.constructGetRequests(rowKey, projectionCriteria);
                result = hBaseClient.batchGet(Lists.newArrayList(get))[0];
            }
            for (Values values : rowToTupleMapper.toValues(tuple, result)) {
                this.collector.emit(tuple, values);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        rowToTupleMapper.declareOutputFields(outputFieldsDeclarer);
    }
}
