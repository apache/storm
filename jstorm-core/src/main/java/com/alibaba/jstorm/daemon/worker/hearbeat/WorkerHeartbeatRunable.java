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
package com.alibaba.jstorm.daemon.worker.hearbeat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * worker Heartbeat
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerHeartbeatRunable extends RunnableCallback {
    private static Logger LOG = LoggerFactory
            .getLogger(WorkerHeartbeatRunable.class);

    private WorkerData workerData;

    private AtomicBoolean shutdown;
    private Map<Object, Object> conf;
    private String worker_id;
    private Integer port;
    private String topologyId;
    private CopyOnWriteArraySet<Integer> task_ids;
    // private Object lock = new Object();

    private Integer frequence;

    private Map<String, LocalState> workerStates;

    public WorkerHeartbeatRunable(WorkerData workerData) {

        this.workerData = workerData;

        this.conf = workerData.getStormConf();
        this.worker_id = workerData.getWorkerId();
        this.port = workerData.getPort();
        this.topologyId = workerData.getTopologyId();
        this.task_ids =
                new CopyOnWriteArraySet<Integer>(workerData.getTaskids());
        this.shutdown = workerData.getShutdown();

        String key = Config.WORKER_HEARTBEAT_FREQUENCY_SECS;
        frequence = JStormUtils.parseInt(conf.get(key), 10);

        this.workerStates = new HashMap<String, LocalState>();
    }

    private LocalState getWorkerState() throws IOException {
        LocalState state = workerStates.get(worker_id);
        if (state == null) {
            state = StormConfig.worker_state(conf, worker_id);
            workerStates.put(worker_id, state);
        }
        return state;
    }

    /**
     * do hearbeat, update LocalState
     * 
     * @throws IOException
     */

    public void doHeartbeat() throws IOException {

        int currtime = TimeUtils.current_time_secs();
        WorkerHeartbeat hb =
                new WorkerHeartbeat(currtime, topologyId, task_ids, port);

        LOG.debug("Doing heartbeat:" + worker_id + ",port:" + port + ",hb"
                + hb.toString());

        LocalState state = getWorkerState();
        state.put(Common.LS_WORKER_HEARTBEAT, hb);

    }

    @Override
    public void run() {

        try {
            doHeartbeat();
        } catch (IOException e) {
            LOG.error("work_heart_beat_fn fail", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public Object getResult() {
        return frequence;
    }
}
