/*
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

package org.apache.storm.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;

public class SupervisorResources {
    private final double totalMem;
    private final double totalCpu;
    private final double usedMem;
    private final double usedCpu;
    private Map<String, Double> totalGenericResources;
    private Map<String, Double> usedGenericResources;

    /**
     * Constructor for a Supervisor's resources.
     *
     * @param totalMem the total mem on the supervisor
     * @param totalCpu the total CPU on the supervisor
     * @param totalGenericResources the total generic resources on the supervisor
     * @param usedMem  the used mem on the supervisor
     * @param usedCpu  the used CPU on the supervisor
     * @param usedGenericResources the used generic resources on the supervisor
     */
    public SupervisorResources(double totalMem, double totalCpu, Map<String, Double> totalGenericResources,
                               double usedMem, double usedCpu, Map<String, Double> usedGenericResources) {
        this.totalMem = totalMem;
        this.totalCpu = totalCpu;
        this.usedMem = usedMem;
        this.usedCpu = usedCpu;
        this.totalGenericResources = totalGenericResources != null ? totalGenericResources : new HashMap<>();
        this.usedGenericResources = usedGenericResources != null ? usedGenericResources : new HashMap<>();
    }

    public double getUsedMem() {
        return usedMem;
    }

    public double getUsedCpu() {
        return usedCpu;
    }

    public double getTotalMem() {
        return totalMem;
    }

    public double getTotalCpu() {
        return totalCpu;
    }

    public double getAvailableCpu() {
        return totalCpu - usedCpu;
    }

    public double getAvailableMem() {
        return totalMem - usedMem;
    }

    public Map<String, Double> getTotalGenericResources() {
        return new HashMap<>(totalGenericResources);
    }

    public Map<String, Double> getUsedGenericResources() {
        return new HashMap<>(usedGenericResources);
    }

    public SupervisorResources add(WorkerResources wr) {
        usedGenericResources = NormalizedResourceRequest.addResourceMap(usedGenericResources, wr.get_resources());
        NormalizedResourceRequest.removeNonGenericResources(usedGenericResources);

        return new SupervisorResources(
                totalMem,
                totalCpu,
                getTotalGenericResources(),
                usedMem + wr.get_mem_off_heap() + wr.get_mem_on_heap(),
                usedCpu + wr.get_cpu(),
                getUsedGenericResources());
    }

    public SupervisorResources addMem(Double value) {
        return new SupervisorResources(totalMem, totalCpu, getTotalGenericResources(),
                usedMem + value, usedCpu, getUsedGenericResources());
    }
}
