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

package org.apache.storm.daemon.supervisor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;

public class StandaloneSupervisor implements ISupervisor {
    private String supervisorId;
    private Map<String, Object> conf;

    @Override
    public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {
        try {
            LocalState localState = new LocalState(schedulerLocalDir, true);
            String supervisorId = localState.getSupervisorId();
            if (supervisorId == null) {
                supervisorId = generateSupervisorId();
                localState.setSupervisorId(supervisorId);
            }
            this.conf = topoConf;
            this.supervisorId = supervisorId;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSupervisorId() {
        return supervisorId;
    }

    @Override
    public String getAssignmentId() {
        return supervisorId;
    }

    @Override
    public Object getMetadata() {
        Object ports = conf.get(DaemonConfig.SUPERVISOR_SLOTS_PORTS);
        return ports;
    }

    @Override
    public boolean confirmAssigned(int port) {
        return true;
    }

    @Override
    public void killedWorker(int port) {

    }

    @Override
    public void assigned(Collection<Integer> ports) {

    }

    public String generateSupervisorId() {
        String extraPart = "";
        try {
            extraPart = "-" + InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            // This should not happen (localhost), but if it does we are still OK
        }
        return Utils.uuid() + extraPart;
    }
}
