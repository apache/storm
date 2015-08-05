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
package com.alibaba.jstorm.daemon.supervisor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * supervisor Heartbeat, just write SupervisorInfo to ZK
 */
class Heartbeat extends RunnableCallback {

    private static Logger LOG = LoggerFactory.getLogger(Heartbeat.class);

    private static final int CPU_THREADHOLD = 4;
    private static final long MEM_THREADHOLD = 8 * JStormUtils.SIZE_1_G;

    private Map<Object, Object> conf;

    private StormClusterState stormClusterState;

    private String supervisorId;

    private String myHostName;

    private final int startTime;

    private final int frequence;

    private SupervisorInfo supervisorInfo;

    private AtomicBoolean hbUpdateTrigger;

    /**
     * @param conf
     * @param stormClusterState
     * @param supervisorId
     * @param myHostName
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Heartbeat(Map conf, StormClusterState stormClusterState,
            String supervisorId) {

        String myHostName = JStormServerUtils.getHostName(conf);

        this.stormClusterState = stormClusterState;
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.myHostName = myHostName;
        this.startTime = TimeUtils.current_time_secs();
        this.frequence =
                JStormUtils.parseInt(conf
                        .get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
        this.hbUpdateTrigger = new AtomicBoolean(true);

        initSupervisorInfo(conf);

        LOG.info("Successfully init supervisor heartbeat thread, "
                + supervisorInfo);
    }

    private void initSupervisorInfo(Map conf) {
        List<Integer> portList = JStormUtils.getSupervisorPortList(conf);

        if (!StormConfig.local_mode(conf)) {
            try {

                boolean isLocaliP = false;
                isLocaliP = myHostName.equals("127.0.0.1");
                if(isLocaliP){
                    throw new Exception("the hostname which  supervisor get is localhost");
                }
            }catch(Exception e1){
                LOG.error("get supervisor host error!", e1);
                throw new RuntimeException(e1);
            }
            Set<Integer> ports = JStormUtils.listToSet(portList);
            supervisorInfo =
                    new SupervisorInfo(myHostName, supervisorId, ports);
        } else {
            Set<Integer> ports = JStormUtils.listToSet(portList.subList(0, 1));
            supervisorInfo =
                    new SupervisorInfo(myHostName, supervisorId, ports);
        }
    }

    @SuppressWarnings("unchecked")
    public void update() {
        supervisorInfo.setTimeSecs(TimeUtils.current_time_secs());
        supervisorInfo
                .setUptimeSecs((int) (TimeUtils.current_time_secs() - startTime));

        try {
            stormClusterState
                    .supervisor_heartbeat(supervisorId, supervisorInfo);
        } catch (Exception e) {
            LOG.error("Failed to update SupervisorInfo to ZK");

        }
    }

    @Override
    public Object getResult() {
        return frequence;
    }

    @Override
    public void run() {
        boolean updateHb = hbUpdateTrigger.getAndSet(false);
        if (updateHb) {
            update();
        }
    }

    public int getStartTime() {
        return startTime;
    }

    public SupervisorInfo getSupervisorInfo() {
        return supervisorInfo;
    }

    public void updateHbTrigger(boolean update) {
        hbUpdateTrigger.set(update);
    }
}
