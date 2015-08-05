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
package com.alibaba.jstorm.task.execute.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class SpoutTimeoutCallBack<K, V> implements ExpiredCallback<K, V> {
    private static Logger LOG = LoggerFactory
            .getLogger(SpoutTimeoutCallBack.class);

    private DisruptorQueue disruptorEventQueue;
    private backtype.storm.spout.ISpout spout;
    private Map storm_conf;
    private TaskBaseMetric task_stats;
    private boolean isDebug;

    public SpoutTimeoutCallBack(DisruptorQueue disruptorEventQueue,
            backtype.storm.spout.ISpout _spout, Map _storm_conf,
            TaskBaseMetric stat) {
        this.storm_conf = _storm_conf;
        this.disruptorEventQueue = disruptorEventQueue;
        this.spout = _spout;
        this.task_stats = stat;
        this.isDebug =
                JStormUtils.parseBoolean(storm_conf.get(Config.TOPOLOGY_DEBUG),
                        false);
    }

    /**
     * pending.put(root_id, JStormUtils.mk_list(message_id, TupleInfo, ms));
     */
    @Override
    public void expire(K key, V val) {
        if (val == null) {
            return;
        }
        try {
            TupleInfo tupleInfo = (TupleInfo) val;
            FailSpoutMsg fail =
                    new FailSpoutMsg(key, spout, (TupleInfo) tupleInfo,
                            task_stats, isDebug);

            disruptorEventQueue.publish(fail);
        } catch (Exception e) {
            LOG.error("expire error", e);
        }
    }
}
