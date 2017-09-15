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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.daemon.Acker;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyDetails {
    private final String topologyId;
    private final Map<String, Object> topologyConf;
    private final StormTopology topology;
    private final Map<ExecutorDetails, String> executorToComponent;
    private final int numWorkers;
    //<ExecutorDetails - Task, Map<String - Type of resource, Map<String - type of that resource, Double - amount>>>
    private Map<ExecutorDetails, Map<String, Double>> resourceList;
    //Max heap size for a worker used by topology
    private Double topologyWorkerMaxHeapSize;
    //topology priority
    private Integer topologyPriority;
    //when topology was launched
    private final int launchTime;
    private final String owner;
    private final String topoName;

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDetails.class);

    public TopologyDetails(String topologyId, Map<String, Object> topologyConf, StormTopology topology, int numWorkers, String owner) {
        this(topologyId, topologyConf, topology,  numWorkers,  null, 0, owner);
    }

    public TopologyDetails(String topologyId, Map<String, Object> topologyConf, StormTopology topology,
                           int numWorkers, Map<ExecutorDetails, String> executorToComponents, String owner) {
        this(topologyId, topologyConf, topology,  numWorkers,  executorToComponents, 0, owner);
    }

    public TopologyDetails(String topologyId, Map<String, Object> topologyConf, StormTopology topology, int numWorkers,
                           Map<ExecutorDetails, String> executorToComponents, int launchTime, String owner) {
        this.owner = owner;
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;
        this.executorToComponent = new HashMap<>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
        if (topology != null) {
            initResourceList();
        }
        initConfigs();
        this.launchTime = launchTime;
        this.topoName = (String) topologyConf.get(Config.TOPOLOGY_NAME);
    }

    public String getId() {
        return topologyId;
    }

    public String getName() {
        return topoName;
    }

    public Map<String, Object> getConf() {
        return topologyConf;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public StormTopology getTopology() {
        return topology;
    }

    public Map<ExecutorDetails, String> getExecutorToComponent() {
        return executorToComponent;
    }

    public Map<ExecutorDetails, String> selectExecutorToComponent(
        Collection<ExecutorDetails> executors) {
        Map<ExecutorDetails, String> ret = new HashMap<>(executors.size());
        for (ExecutorDetails executor : executors) {
            String compId = executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }

        return ret;
    }

    public Set<ExecutorDetails> getExecutors() {
        return executorToComponent.keySet();
    }

    private void initResourceList() {
        this.resourceList = new HashMap<>();
        // Extract bolt memory info
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                //the json_conf is populated by TopologyBuilder (e.g. boltDeclarer.setMemoryLoad)
                Map<String, Double> topologyResources =
                    ResourceUtils.parseResources(bolt.getValue().get_common().get_json_conf());
                ResourceUtils.checkIntialization(topologyResources, bolt.getKey(), topologyConf);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent :
                    executorToComponent.entrySet()) {
                    if (bolt.getKey().equals(anExecutorToComponent.getValue())) {
                        resourceList.put(anExecutorToComponent.getKey(), topologyResources);
                    }
                }
            }
        }
        // Extract spout memory info
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                Map<String, Double> topologyResources =
                    ResourceUtils.parseResources(spout.getValue().get_common().get_json_conf());
                ResourceUtils.checkIntialization(topologyResources, spout.getKey(), this.topologyConf);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent :
                    executorToComponent.entrySet()) {
                    if (spout.getKey().equals(anExecutorToComponent.getValue())) {
                        resourceList.put(anExecutorToComponent.getKey(), topologyResources);
                    }
                }
            }
        } else {
            LOG.warn("Topology " + topologyId + " does not seem to have any spouts!");
        }
        //schedule tasks that are not part of components returned from topology.get_spout or
        // topology.getbolt (AKA sys tasks most specifically __acker tasks)
        for (ExecutorDetails exec : getExecutors()) {
            if (!resourceList.containsKey(exec)) {
                addDefaultResforExec(exec);
            }
        }
    }

    private List<ExecutorDetails> componentToExecs(String comp) {
        List<ExecutorDetails> execs = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponent.entrySet()) {
            if (entry.getValue().equals(comp)) {
                execs.add(entry.getKey());
            }
        }
        return execs;
    }

    private Set<String> getInputsTo(ComponentCommon comp) {
        Set<String> ret = new HashSet<>();
        for (GlobalStreamId globalId : comp.get_inputs().keySet()) {
            ret.add(globalId.get_componentId());
        }
        return ret;
    }

    /**
     * Returns a representation of the non-system components of the topology graph Each Component
     * object in the returning map is populated with the list of its parents, children and execs
     * assigned to that component.
     *
     * @return a map of components
     */
    public Map<String, Component> getComponents() {
        Map<String, Component> ret = new HashMap<>();

        Map<String, SpoutSpec> spouts = topology.get_spouts();
        Map<String, Bolt> bolts = topology.get_bolts();
        //Add in all of the components
        if (spouts != null) {
            for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
                String compId = entry.getKey();
                if (!Utils.isSystemId(compId)) {
                    Component comp = new Component(ComponentType.SPOUT, compId, componentToExecs(compId));
                    ret.put(compId, comp);
                }
            }
        }
        if (bolts != null) {
            for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
                String compId = entry.getKey();
                if (!Utils.isSystemId(compId)) {
                    Component comp = new Component(ComponentType.BOLT, compId, componentToExecs(compId));
                    ret.put(compId, comp);
                }
            }
        }

        //Link the components together
        if (spouts != null) {
            for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
                Component spout = ret.get(entry.getKey());
                for (String parentId : getInputsTo(entry.getValue().get_common())) {
                    ret.get(parentId).addChild(spout);
                }
            }
        }

        if (bolts != null) {
            for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
                Component bolt = ret.get(entry.getKey());
                for (String parentId : getInputsTo(entry.getValue().get_common())) {
                    ret.get(parentId).addChild(bolt);
                }
            }
        }
        return ret;
    }

    /**
     * Gets the on heap memory requirement for a certain task within a topology.
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of on heap memory requirement for this exec in topology topoId.
     */
    public Double getOnHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            ret = resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        }
        return ret;
    }

    /**
     * Gets the off heap memory requirement for a certain task within a topology.
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of off heap memory requirement for this exec in topology topoId.
     */
    public Double getOffHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            ret = resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        }
        return ret;
    }

    /**
     * Gets the total memory requirement for a task.
     * @param exec the executor the inquiry is concerning.
     * @return Double the total memory requirement for this exec in topology topoId.
     */
    public Double getTotalMemReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return getOffHeapMemoryRequirement(exec)
                    + getOnHeapMemoryRequirement(exec);
        }
        return null;
    }

    /**
     * Gets the total memory resource list for a set of tasks that is part of a topology.
     * @return Map<ExecutorDetails, Double> a map of the total memory requirement for all tasks in topology topoId.
     */
    public Map<ExecutorDetails, Double> getTotalMemoryResourceList() {
        Map<ExecutorDetails, Double> ret = new HashMap<>();
        for (ExecutorDetails exec : resourceList.keySet()) {
            ret.put(exec, getTotalMemReqTask(exec));
        }
        return ret;
    }

    public Set<SharedMemory> getSharedMemoryRequests(Collection<ExecutorDetails> executors) {
        Set<String> components = new HashSet<>();
        for (ExecutorDetails exec : executors) {
            String component = executorToComponent.get(exec);
            if (component != null) {
                components.add(component);
            }
        }
        Set<SharedMemory> ret = new HashSet<>();
        if (topology != null) {
            //topology being null is used for tests  We probably should fix that at some point,
            // but it is not trivial to do...
            Map<String, Set<String>> compToSharedName = topology.get_component_to_shared_memory();
            if (compToSharedName != null) {
                for (String component : components) {
                    Set<String> sharedNames = compToSharedName.get(component);
                    if (sharedNames != null) {
                        for (String name : sharedNames) {
                            ret.add(topology.get_shared_memory().get(name));
                        }
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Get the total CPU requirement for executor
     * @return Double the total about of cpu requirement for executor
     */
    public Double getTotalCpuReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
        }
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total on-heap memory requested for this topology
     */
    public double getTotalRequestedMemOnHeap() {
        return getRequestedSharedOnHeap() + getRequestedNonSharedOnHeap();
    }

    public double getRequestedSharedOnHeap() {
        double ret = 0.0;
        if (topology.is_set_shared_memory()) {
            for (SharedMemory req : topology.get_shared_memory().values()) {
                ret += req.get_on_heap();
            }
        }
        return ret;
    }

    public double getRequestedNonSharedOnHeap() {
        double totalMemOnHeap = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double execMem = getOnHeapMemoryRequirement(exec);
            if (execMem != null) {
                totalMemOnHeap += execMem;
            }
        }
        return totalMemOnHeap;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total off-heap memory requested for this topology
     */
    public double getTotalRequestedMemOffHeap() {
        return getRequestedNonSharedOffHeap() + getRequestedSharedOffHeap();
    }

    public double getRequestedNonSharedOffHeap() {
        double totalMemOffHeap = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double execMem = getOffHeapMemoryRequirement(exec);
            if (execMem != null) {
                totalMemOffHeap += execMem;
            }
        }
        return totalMemOffHeap;
    }

    public double getRequestedSharedOffHeap() {
        double ret = 0.0;
        if (topology.is_set_shared_memory()) {
            for (SharedMemory req : topology.get_shared_memory().values()) {
                ret += req.get_off_heap_worker() + req.get_off_heap_node();
            }
        }
        return ret;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return the total cpu requested for this topology
     */
    public double getTotalRequestedCpu() {
        double totalCpu = 0.0;
        for (ExecutorDetails exec : this.getExecutors()) {
            Double execCpu = getTotalCpuReqTask(exec);
            if (execCpu != null) {
                totalCpu += execCpu;
            }
        }
        return totalCpu;
    }

    /**
     * get the resources requirements for a executor
     * @param exec
     * @return a map containing the resource requirements for this exec
     */
    public Map<String, Double> getTaskResourceReqList(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return resourceList.get(exec);
        }
        return null;
    }

    /**
     * Checks if a executor is part of this topology
     * @return Boolean whether or not a certain ExecutorDetail is included in the resourceList.
     */
    public boolean hasExecInTopo(ExecutorDetails exec) {
        return resourceList != null && resourceList.containsKey(exec);
    }

    /**
     * add resource requirements for a executor
     */
    public void addResourcesForExec(ExecutorDetails exec, Map<String, Double> resourceList) {
        if (hasExecInTopo(exec)) {
            LOG.warn("Executor {} already exists...ResourceList: {}", exec, getTaskResourceReqList(exec));
            return;
        }
        this.resourceList.put(exec, resourceList);
    }

    /**
     * Add default resource requirements for a executor
     */
    private void addDefaultResforExec(ExecutorDetails exec) {
        Double topologyComponentCpuPcorePercent =
            ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
        Double topologyComponentResourcesOffheapMemoryMb =
            ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
        Double topologyComponentResourcesOnheapMemoryMb =
            ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);

        assert topologyComponentCpuPcorePercent != null;
        assert topologyComponentResourcesOffheapMemoryMb != null;
        assert topologyComponentResourcesOnheapMemoryMb != null;

        Map<String, Double> defaultResourceList = new HashMap<>();
        defaultResourceList.put(
            Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topologyComponentCpuPcorePercent);
        defaultResourceList.put(
            Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
            topologyComponentResourcesOffheapMemoryMb);
        defaultResourceList.put(
            Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
            topologyComponentResourcesOnheapMemoryMb);

        adjustResourcesForExec(exec, defaultResourceList);

        LOG.debug(
            "Scheduling Executor: {} {} with memory requirement as onHeap: {} - offHeap: {} "
                + "and CPU requirement: {}",
            getExecutorToComponent().get(exec),
            exec,
            topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB),
            topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB),
            topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));

        addResourcesForExec(exec, defaultResourceList);
    }

    /**
     * Some components might have different resource configs.
     */
    private void adjustResourcesForExec(ExecutorDetails exec, Map<String, Double> resourceListForExec) {
        String component = getExecutorToComponent().get(exec);
        if (component.equals(Acker.ACKER_COMPONENT_ID)) {
            if (topologyConf.containsKey(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB)));
            }
            if (topologyConf.containsKey(Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB)));
            }
            if (topologyConf.containsKey(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT)));
            }
        } else if (component.startsWith(Constants.METRICS_COMPONENT_ID_PREFIX)) {
            if (topologyConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_ONHEAP_MEMORY_MB)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_ONHEAP_MEMORY_MB)));
            }
            if (topologyConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_OFFHEAP_MEMORY_MB)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_OFFHEAP_MEMORY_MB)));
            }
            if (topologyConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_CPU_PCORE_PERCENT)) {
                resourceListForExec.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                        ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_METRICS_CONSUMER_CPU_PCORE_PERCENT)));
            }
        }
    }

    /**
     * initializes member variables
     */
    private void initConfigs() {
        this.topologyWorkerMaxHeapSize =
            ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB), null);
        this.topologyPriority =
            ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_PRIORITY), null);

        assert this.topologyWorkerMaxHeapSize != null;
        assert this.topologyPriority != null;
    }

    /**
     * Get the max heap size for a worker used by this topology
     * @return the worker max heap size
     */
    public Double getTopologyWorkerMaxHeapSize() {
        return topologyWorkerMaxHeapSize;
    }

    /**
     * Get the user that submitted this topology
     */
    public String getTopologySubmitter() {
        return owner;
    }

    /**
     * get the priority of this topology
     */
    public int getTopologyPriority() {
        return topologyPriority;
    }

    /**
     * Get the timestamp of when this topology was launched
     */
    public int getLaunchTime() {
        return launchTime;
    }

    /**
     * Get how long this topology has been executing
     */
    public int getUpTime() {
        return Time.currentTimeSecs() - launchTime;
    }

    @Override
    public String toString() {
        return "Name: "
            + getName()
            + " id: "
            + getId()
            + " Priority: "
            + getTopologyPriority()
            + " Uptime: "
            + getUpTime()
            + " CPU: "
            + getTotalRequestedCpu()
            + " Memory: "
            + (getTotalRequestedMemOffHeap() + getTotalRequestedMemOnHeap());
    }

    @Override
    public int hashCode() {
        return topologyId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopologyDetails)) {
            return false;
        }
        return (topologyId.equals(((TopologyDetails) o).getId()));
    }
}
