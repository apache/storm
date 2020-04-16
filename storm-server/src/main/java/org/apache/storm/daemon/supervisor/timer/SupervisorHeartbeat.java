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

package org.apache.storm.daemon.supervisor.timer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.ServerConstants;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorHeartbeat implements Runnable {

    private final IStormClusterState stormClusterState;
    private final String supervisorId;
    private final Map<String, Object> conf;
    private final Supervisor supervisor;

    public static final Logger LOG = LoggerFactory.getLogger(SupervisorHeartbeat.class);


    public SupervisorHeartbeat(Map<String, Object> conf, Supervisor supervisor) {
        this.stormClusterState = supervisor.getStormClusterState();
        this.supervisorId = supervisor.getId();
        this.supervisor = supervisor;
        this.conf = conf;
    }

    private Map<String, SupervisorInfo> buildSupervisorInfo(Map<String, Object> conf, Supervisor supervisor,
                                                            Map<String, Object> validatedNumaMap) {
        List metaDatas = (List) supervisor.getiSupervisor().getMetadata();
        List<Long> allPortList = new ArrayList<>();
        if (metaDatas != null) {
            for (Object data : metaDatas) {
                Integer port = ObjectReader.getInt(data);
                if (port != null) {
                    allPortList.add(port.longValue());
                }
            }
        }

        List<Long> allUsedPorts = new ArrayList<>();
        allUsedPorts.addAll(supervisor.getCurrAssignment().get().keySet());
        Map<String, Double> totalSupervisorResources = mkSupervisorCapacities(conf);
        NormalizedResourceOffer totalSupervisorNormalizedResources = new NormalizedResourceOffer(totalSupervisorResources);
       
        Map<String, SupervisorInfo> result = new HashMap();

        if (validatedNumaMap != null) {
            for (Map.Entry<String, Object> numaMapEntry : validatedNumaMap.entrySet()) {
                SupervisorInfo supervisorInfo = new SupervisorInfo();
                supervisorInfo.set_time_secs(Time.currentTimeSecs());
                supervisorInfo.set_hostname(supervisor.getHostName());
                supervisorInfo.set_assignment_id(
                        supervisor.getAssignmentId() + ServerConstants.NUMA_ID_SEPARATOR + numaMapEntry.getKey()
                );
                supervisorInfo.set_server_port(supervisor.getThriftServerPort());

                Map<String, Object> numaMap = (Map<String, Object>) numaMapEntry.getValue();
                List numaPortList = ((List<Integer>) numaMap.get(ServerConstants.NUMA_PORTS)).stream()
                    .map(e -> e.longValue()).collect(Collectors.toList());

                List<Long> usedNumaPorts = ListUtils.intersection(numaPortList, allUsedPorts);
                supervisorInfo.set_used_ports(usedNumaPorts);
                supervisorInfo.set_meta(numaPortList);
                allPortList = ListUtils.subtract(allPortList, numaPortList);
                allUsedPorts = ListUtils.subtract(allUsedPorts, usedNumaPorts);
                supervisorInfo.set_scheduler_meta(
                        (Map<String, String>) conf.get(DaemonConfig.SUPERVISOR_SCHEDULER_META)
                );
                supervisorInfo.set_uptime_secs(supervisor.getUpTime().upTime());
                supervisorInfo.set_version(supervisor.getStormVersion());
                Map<String, Double> supervisorCapacitiesFromNumaMap = mkSupervisorCapacitiesFromNumaMap(numaMap);
                NormalizedResourceOffer numaNormalizedResources = new NormalizedResourceOffer(supervisorCapacitiesFromNumaMap);
                totalSupervisorNormalizedResources.remove(numaNormalizedResources);
                supervisorInfo.set_resources_map(supervisorCapacitiesFromNumaMap);
                result.put(supervisor.getId() + ServerConstants.NUMA_ID_SEPARATOR + numaMapEntry.getKey(), supervisorInfo);
            }
        }

        if (totalSupervisorNormalizedResources.getTotalCpu() > 0
            && totalSupervisorNormalizedResources.getTotalMemoryMb() > 0 && !allPortList.isEmpty()) {
            SupervisorInfo supervisorInfo = new SupervisorInfo();
            supervisorInfo.set_time_secs(Time.currentTimeSecs());
            supervisorInfo.set_hostname(supervisor.getHostName());
            supervisorInfo.set_assignment_id(supervisor.getAssignmentId());
            supervisorInfo.set_server_port(supervisor.getThriftServerPort());
            supervisorInfo.set_used_ports(allUsedPorts);
            supervisorInfo.set_meta(allPortList);
            supervisorInfo.set_scheduler_meta((Map<String, String>) conf.get(DaemonConfig.SUPERVISOR_SCHEDULER_META));
            supervisorInfo.set_uptime_secs(supervisor.getUpTime().upTime());
            supervisorInfo.set_version(supervisor.getStormVersion());
            supervisorInfo.set_resources_map(totalSupervisorNormalizedResources.toNormalizedMap());
            result.put(supervisor.getId(), supervisorInfo);
        }
        return result;
    }

    private Map<String, Double> mkSupervisorCapacitiesFromNumaMap(Map<String, Object> numaMap) {
        Map<String, Double> ret = new HashMap();
        ret.put(
                Config.SUPERVISOR_CPU_CAPACITY,
                (double) (((List<Integer>) numaMap.get(ServerConstants.NUMA_CORES)).size() * 100)
        );
        ret.put(
                Config.SUPERVISOR_MEMORY_CAPACITY_MB,
                Double.valueOf((Integer) numaMap.get(ServerConstants.NUMA_MEMORY_IN_MB))
        );
        ret.putAll((Map<String, Double>) numaMap.getOrDefault(ServerConstants.NUMA_GENERIC_RESOURCES_MAP, Collections.emptyMap()));
        return NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(ret);
    }

    private Map<String, Double> mkSupervisorCapacities(Map<String, Object> conf) {
        Map<String, Double> ret = new HashMap<String, Double>();
        // Put in legacy values
        Double mem = ObjectReader.getDouble(conf.get(Config.SUPERVISOR_MEMORY_CAPACITY_MB), 4096.0);
        ret.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, mem);
        Double cpu = ObjectReader.getDouble(conf.get(Config.SUPERVISOR_CPU_CAPACITY), 400.0);
        ret.put(Config.SUPERVISOR_CPU_CAPACITY, cpu);


        // If configs are present in Generic map and legacy - the legacy values will be overwritten
        Map<String, Number> rawResourcesMap = (Map<String, Number>) conf.getOrDefault(
            Config.SUPERVISOR_RESOURCES_MAP, Collections.emptyMap()
        );

        for (Map.Entry<String, Number> stringNumberEntry : rawResourcesMap.entrySet()) {
            ret.put(stringNumberEntry.getKey(), stringNumberEntry.getValue().doubleValue());
        }

        LOG.debug(NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(ret).toString());
        return NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(ret);
    }

    @Override
    public void run() {
        Map<String, Object> validatedNumaMap = SupervisorUtils.getNumaMap(conf);
        Map<String, SupervisorInfo> supervisorInfoList = buildSupervisorInfo(conf, supervisor, validatedNumaMap);
        for (Map.Entry<String, SupervisorInfo> supervisorInfoEntry: supervisorInfoList.entrySet()) {
            stormClusterState.supervisorHeartbeat(supervisorInfoEntry.getKey(), supervisorInfoEntry.getValue());
        }
    }
}
