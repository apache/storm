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
package com.alibaba.jstorm.schedule.default_assign;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class WorkerScheduler {

    public static Logger LOG = LoggerFactory.getLogger(WorkerScheduler.class);

    private static WorkerScheduler instance;

    private WorkerScheduler() {

    }

    public static WorkerScheduler getInstance() {
        if (instance == null) {
            instance = new WorkerScheduler();
        }
        return instance;
    }

    public List<ResourceWorkerSlot> getAvailableWorkers(
            DefaultTopologyAssignContext context, Set<Integer> needAssign,
            int num) {
        int workersNum = getWorkersNum(context, num);
        if (workersNum == 0) {
            throw new FailedAssignTopologyException("there's no enough worker");
        }
        List<ResourceWorkerSlot> assignedWorkers =
                new ArrayList<ResourceWorkerSlot>();
        // userdefine assignments
        getRightWorkers(
                context,
                needAssign,
                assignedWorkers,
                workersNum,
                getUserDefineWorkers(context, ConfigExtension
                        .getUserDefineAssignment(context.getStormConf())));
        // old assignments
        if (ConfigExtension.isUseOldAssignment(context.getStormConf())) {
            getRightWorkers(context, needAssign, assignedWorkers, workersNum,
                    context.getOldWorkers());
        } else if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE
                && context.isReassign() == false) {
            int cnt = 0;
            for (ResourceWorkerSlot worker : context.getOldWorkers()) {
                if (cnt < workersNum) {
                    ResourceWorkerSlot resFreeWorker = new ResourceWorkerSlot();
                    resFreeWorker.setPort(worker.getPort());
                    resFreeWorker.setHostname(worker.getHostname());
                    resFreeWorker.setNodeId(worker.getNodeId());
                    assignedWorkers.add(resFreeWorker);
                    cnt++;
                } else {
                    break;
                }
            }
        }
        int defaultWorkerNum =
                Math.min(workersNum - assignedWorkers.size(), needAssign.size());
        LOG.info("Get workers from user define and old assignments: "
                + assignedWorkers);
        for (int i = 0; i < defaultWorkerNum; i++) {
            assignedWorkers.add(new ResourceWorkerSlot());
        }
        List<SupervisorInfo> isolationSupervisors =
                this.getIsolationSupervisors(context);
        if (isolationSupervisors.size() != 0) {
            putAllWorkerToSupervisor(assignedWorkers,
                    getResAvailSupervisors(isolationSupervisors));
        } else {
            putAllWorkerToSupervisor(assignedWorkers,
                    getResAvailSupervisors(context.getCluster()));
        }
        this.setAllWorkerMemAndCpu(context.getStormConf(), assignedWorkers);
        LOG.info("Assigned workers=" + assignedWorkers);
        return assignedWorkers;
    }

    private void setAllWorkerMemAndCpu(Map conf,
            List<ResourceWorkerSlot> assignedWorkers) {
        long defaultSize = ConfigExtension.getMemSizePerWorker(conf);
        int defaultCpu = ConfigExtension.getCpuSlotPerWorker(conf);
        for (ResourceWorkerSlot worker : assignedWorkers) {
            if (worker.getMemSize() <= 0)
                worker.setMemSize(defaultSize);
            if (worker.getCpu() <= 0)
                worker.setCpu(defaultCpu);
        }
    }

    private void putAllWorkerToSupervisor(
            List<ResourceWorkerSlot> assignedWorkers,
            List<SupervisorInfo> supervisors) {
        for (ResourceWorkerSlot worker : assignedWorkers) {
            if (worker.getHostname() != null) {
                for (SupervisorInfo supervisor : supervisors) {
                    if (NetWorkUtils.equals(supervisor.getHostName(),
                            worker.getHostname())
                            && supervisor.getAvailableWorkerPorts().size() > 0) {
                        putWorkerToSupervisor(supervisor, worker);
                        break;
                    }
                }
            }
        }
        supervisors = getResAvailSupervisors(supervisors);
        Collections.sort(supervisors, new Comparator<SupervisorInfo>() {

            @Override
            public int compare(SupervisorInfo o1, SupervisorInfo o2) {
                // TODO Auto-generated method stub
                return -NumberUtils.compare(o1.getAvailableWorkerPorts().size(), o2
                        .getAvailableWorkerPorts().size());
            }

        });
        putWorkerToSupervisor(assignedWorkers, supervisors);
    }

    private void putWorkerToSupervisor(SupervisorInfo supervisor,
            ResourceWorkerSlot worker) {
        int port = worker.getPort();
        if (!supervisor.getAvailableWorkerPorts().contains(worker.getPort())) {
            port = supervisor.getAvailableWorkerPorts().iterator().next();
        }
        worker.setPort(port);
        supervisor.getAvailableWorkerPorts().remove(port);
        worker.setNodeId(supervisor.getSupervisorId());
    }

    private void putWorkerToSupervisor(
            List<ResourceWorkerSlot> assignedWorkers,
            List<SupervisorInfo> supervisors) {
        int allUsedPorts = 0;
        for (SupervisorInfo supervisor : supervisors) {
            int supervisorUsedPorts = supervisor.getWorkerPorts().size()
                    - supervisor.getAvailableWorkerPorts().size();
            allUsedPorts = allUsedPorts + supervisorUsedPorts;
        }
        // per supervisor should be allocated ports in theory
        int theoryAveragePorts =
                (allUsedPorts + assignedWorkers.size()) / supervisors.size()
                        + 1;
        // supervisor which use more than theoryAveragePorts ports will be
        // pushed overLoadSupervisors
        List<SupervisorInfo> overLoadSupervisors =
                new ArrayList<SupervisorInfo>();
        int key = 0;
        Iterator<ResourceWorkerSlot> iterator = assignedWorkers.iterator();
        while (iterator.hasNext()) {
            if (supervisors.size() == 0)
                break;
            if (key >= supervisors.size())
                key = 0;
            SupervisorInfo supervisor = supervisors.get(key);
            int supervisorUsedPorts = supervisor.getWorkerPorts().size()
                    - supervisor.getAvailableWorkerPorts().size();
            if (supervisorUsedPorts < theoryAveragePorts) {
                ResourceWorkerSlot worker = iterator.next();
                if (worker.getNodeId() != null)
                    continue;
                worker.setHostname(supervisor.getHostName());
                worker.setNodeId(supervisor.getSupervisorId());
                worker.setPort(
                        supervisor.getAvailableWorkerPorts().iterator().next());
                supervisor.getAvailableWorkerPorts().remove(worker.getPort());
                if (supervisor.getAvailableWorkerPorts().size() == 0)
                    supervisors.remove(supervisor);
                key++;
            } else {
                overLoadSupervisors.add(supervisor);
                supervisors.remove(supervisor);
            }
        }
        // rest assignedWorkers will be allocate supervisor by deal
        Collections.sort(overLoadSupervisors, new Comparator<SupervisorInfo>() {

            @Override
            public int compare(SupervisorInfo o1, SupervisorInfo o2) {
                // TODO Auto-generated method stub
                return -NumberUtils.compare(o1.getAvailableWorkerPorts().size(),
                        o2.getAvailableWorkerPorts().size());
            }

        });
        key = 0;
        while (iterator.hasNext()) {
            if (overLoadSupervisors.size() == 0)
                break;
            if (key >= overLoadSupervisors.size())
                key = 0;
            ResourceWorkerSlot worker = iterator.next();
            if (worker.getNodeId() != null)
                continue;
            SupervisorInfo supervisor = overLoadSupervisors.get(key);
            worker.setHostname(supervisor.getHostName());
            worker.setNodeId(supervisor.getSupervisorId());
            worker.setPort(
                    supervisor.getAvailableWorkerPorts().iterator().next());
            supervisor.getAvailableWorkerPorts().remove(worker.getPort());
            if (supervisor.getAvailableWorkerPorts().size() == 0)
                overLoadSupervisors.remove(supervisor);
            key++;
        }
    }

    private void getRightWorkers(DefaultTopologyAssignContext context,
            Set<Integer> needAssign, List<ResourceWorkerSlot> assignedWorkers,
            int workersNum, Collection<ResourceWorkerSlot> workers) {
        Set<Integer> assigned = new HashSet<Integer>();
        List<ResourceWorkerSlot> users = new ArrayList<ResourceWorkerSlot>();
        if (workers == null)
            return;
        for (ResourceWorkerSlot worker : workers) {
            boolean right = true;
            Set<Integer> tasks = worker.getTasks();
            if (tasks == null)
                continue;
            for (Integer task : tasks) {
                if (!needAssign.contains(task) || assigned.contains(task)) {
                    right = false;
                    break;
                }
            }
            if (right) {
                assigned.addAll(tasks);
                users.add(worker);
            }
        }
        if (users.size() + assignedWorkers.size() > workersNum) {
            return;
        }

        if (users.size() + assignedWorkers.size() == workersNum
                && assigned.size() != needAssign.size()) {
            return;
        }
        assignedWorkers.addAll(users);
        needAssign.removeAll(assigned);
    }

    private int getWorkersNum(DefaultTopologyAssignContext context,
            int workersNum) {
        Map<String, SupervisorInfo> supervisors = context.getCluster();
        List<SupervisorInfo> isolationSupervisors =
                this.getIsolationSupervisors(context);
        int slotNum = 0;

        if (isolationSupervisors.size() != 0) {
            for (SupervisorInfo superivsor : isolationSupervisors) {
                slotNum = slotNum + superivsor.getAvailableWorkerPorts().size();
            }
            return Math.min(slotNum, workersNum);
        }
        for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
            slotNum = slotNum + entry.getValue().getAvailableWorkerPorts().size();
        }
        return Math.min(slotNum, workersNum);
    }

    /**
     * @param context
     * @param workers
     * @return
     */
    private List<ResourceWorkerSlot> getUserDefineWorkers(
            DefaultTopologyAssignContext context, List<WorkerAssignment> workers) {
        List<ResourceWorkerSlot> ret = new ArrayList<ResourceWorkerSlot>();
        if (workers == null)
            return ret;
        Map<String, List<Integer>> componentToTask =
                (HashMap<String, List<Integer>>) ((HashMap<String, List<Integer>>) context
                        .getComponentTasks()).clone();
        if (context.getAssignType() != context.ASSIGN_TYPE_NEW) {
            checkUserDefineWorkers(context, workers,
                    context.getTaskToComponent());
        }
        for (WorkerAssignment worker : workers) {
            ResourceWorkerSlot workerSlot =
                    new ResourceWorkerSlot(worker, componentToTask);
            if (workerSlot.getTasks().size() != 0) {
                ret.add(workerSlot);
            }
        }
        return ret;
    }

    private void checkUserDefineWorkers(DefaultTopologyAssignContext context,
            List<WorkerAssignment> workers, Map<Integer, String> taskToComponent) {
        Set<ResourceWorkerSlot> unstoppedWorkers =
                context.getUnstoppedWorkers();
        List<WorkerAssignment> re = new ArrayList<WorkerAssignment>();
        for (WorkerAssignment worker : workers) {
            for (ResourceWorkerSlot unstopped : unstoppedWorkers) {
                if (unstopped
                        .compareToUserDefineWorker(worker, taskToComponent))
                    re.add(worker);
            }
        }
        workers.removeAll(re);

    }

    private List<SupervisorInfo> getResAvailSupervisors(
            Map<String, SupervisorInfo> supervisors) {
        List<SupervisorInfo> availableSupervisors =
                new ArrayList<SupervisorInfo>();
        for (Entry<String, SupervisorInfo> entry : supervisors.entrySet()) {
            SupervisorInfo supervisor = entry.getValue();
            if (supervisor.getAvailableWorkerPorts().size() > 0)
                availableSupervisors.add(entry.getValue());
        }
        return availableSupervisors;
    }

    private List<SupervisorInfo> getResAvailSupervisors(
            List<SupervisorInfo> supervisors) {
        List<SupervisorInfo> availableSupervisors =
                new ArrayList<SupervisorInfo>();
        for (SupervisorInfo supervisor : supervisors) {
            if (supervisor.getAvailableWorkerPorts().size() > 0)
                availableSupervisors.add(supervisor);
        }
        return availableSupervisors;
    }

    private List<SupervisorInfo> getIsolationSupervisors(
            DefaultTopologyAssignContext context) {
        List<String> isolationHosts =
                (List<String>) context.getStormConf().get(
                        Config.ISOLATION_SCHEDULER_MACHINES);
        LOG.info("Isolation machines: " + isolationHosts);
        if (isolationHosts == null)
            return new ArrayList<SupervisorInfo>();
        List<SupervisorInfo> isolationSupervisors =
                new ArrayList<SupervisorInfo>();
        for (Entry<String, SupervisorInfo> entry : context.getCluster()
                .entrySet()) {
            if (containTargetHost(isolationHosts, entry.getValue()
                    .getHostName())) {
                isolationSupervisors.add(entry.getValue());
            }
        }
        return isolationSupervisors;
    }

    private boolean containTargetHost(Collection<String> hosts, String target) {
        for (String host : hosts) {
            if (NetWorkUtils.equals(host, target) == true) {
                return true;
            }
        }
        return false;
    }
}
