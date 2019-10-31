/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.supervisor;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.metric.StormMetricsRegistry;

public class ContainerMemoryTracker {

    private final ConcurrentHashMap<Integer, TopoAndMemory> usedMemory = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, TopoAndMemory> reservedMemory = new ConcurrentHashMap<>();

    public ContainerMemoryTracker(StormMetricsRegistry metricsRegistry) {
        metricsRegistry.registerGauge(
            "supervisor:current-used-memory-mb",
            () -> {
                Long val = usedMemory.values().stream().mapToLong((topoAndMem) -> topoAndMem.memory).sum();
                int ret = val.intValue();
                if (val > Integer.MAX_VALUE) { // Would only happen at 2 PB so we are OK for now
                    ret = Integer.MAX_VALUE;
                }
                return ret;
            });
        metricsRegistry.registerGauge(
            "supervisor:current-reserved-memory-mb",
            () -> {
                Long val = reservedMemory.values().stream().mapToLong((topoAndMem) -> topoAndMem.memory).sum();
                int ret = val.intValue();
                if (val > Integer.MAX_VALUE) { // Would only happen at 2 PB so we are OK for now
                    ret = Integer.MAX_VALUE;
                }
                return ret;
            });
    }

    /**
     * Get the memory used by the worker on the given port.
     *
     * @param port The worker port
     * @return The memory used by the worker, or empty if no worker exists on the given port.
     */
    public Optional<Long> getUsedMemoryMb(int port) {
        TopoAndMemory topoAndMemory = usedMemory.get(port);
        if (topoAndMemory == null) {
            return Optional.empty();
        }
        return Optional.of(topoAndMemory.memory);
    }

    /**
     * Gets the memory used by the given topology across all ports on this supervisor.
     *
     * @param topologyId The topology id
     * @return The memory used by the given topology id
     */
    public long getUsedMemoryMb(String topologyId) {
        return usedMemory
            .values()
            .stream()
            .filter((topoAndMem) -> topologyId.equals(topoAndMem.topoId))
            .mapToLong((topoAndMem) -> topoAndMem.memory)
            .sum();
    }

    /**
     * Gets the memory reserved by the given topology across all ports on this supervisor.
     *
     * @param topologyId The topology id
     * @return The memory reserved by the given topology id
     */
    public long getReservedMemoryMb(String topologyId) {
        return reservedMemory
            .values()
            .stream()
            .filter((topoAndMem) -> topologyId.equals(topoAndMem.topoId))
            .mapToLong((topoAndMem) -> topoAndMem.memory)
            .sum();
    }

    /**
     * Gets the number of worker ports assigned to the given topology id on this supervisor.
     *
     * @param topologyId The topology id
     * @return The number of worker ports assigned to the given topology.
     */
    public long getAssignedWorkerCount(String topologyId) {
        return usedMemory
            .values()
            .stream()
            .filter((topoAndMem) -> topologyId.equals(topoAndMem.topoId))
            .count();
    }

    /**
     * Clears the topology assignment and tracked memory for the given port.
     *
     * @param port The worker port
     */
    public void remove(int port) {
        usedMemory.remove(port);
        reservedMemory.remove(port);
    }

    /**
     * Assigns the given topology id to the given port, and sets the used memory for that port and topology id.
     *
     * @param port The worker port
     * @param topologyId The topology id
     * @param usedMemoryMb The memory used by the topology
     */
    public void setUsedMemoryMb(int port, String topologyId, long usedMemoryMb) {
        usedMemory.put(port, new TopoAndMemory(topologyId, usedMemoryMb));
    }

    /**
     * Sets the reserved memory for the given port and topology id.
     *
     * @param port The worker port
     * @param topologyId The topology id
     * @param reservedMemoryMb The memory reserved by the topology
     */
    public void setReservedMemoryMb(int port, String topologyId, long reservedMemoryMb) {
        reservedMemory.put(port, new TopoAndMemory(topologyId, reservedMemoryMb));
    }

    private static class TopoAndMemory {

        public final String topoId;
        public final long memory;

        TopoAndMemory(String id, long mem) {
            topoId = id;
            memory = mem;
        }

        @Override
        public String toString() {
            return "{TOPO: " + topoId + " at " + memory + " MB}";
        }
    }

}
