package com.alibaba.jstorm.schedule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;

@Deprecated
public class DefaultScheduler implements IScheduler{
	
	private Set<WorkerSlot> slotsCanReassign(Cluster cluster, Set<WorkerSlot> slots) {
		Set<WorkerSlot> result = new HashSet<WorkerSlot>();
		for(WorkerSlot slot : slots) {
			if(!cluster.isBlackListed(slot.getNodeId())) {
				if(cluster.getSupervisorById(slot.getNodeId()).getAllPorts().contains(slot.getPort()))
					result.add(slot);
			}
		}
		return result;
	}
	
	private Set<WorkerSlot> badSlots(Map<WorkerSlot, List<ExecutorDetails>> existingSlots, int numExecutors, int numWorkers) {
		if(numWorkers != 0) {
			Map<Integer, Integer> distribution = Utils.integerDivided(numExecutors, numWorkers);
			Map<WorkerSlot, List<ExecutorDetails>> result = new HashMap<WorkerSlot, List<ExecutorDetails>>();
			for(Entry<WorkerSlot, List<ExecutorDetails>> entry : existingSlots.entrySet()) {
				Integer executorCount = distribution.get(entry.getValue().size());
				if(executorCount != null && executorCount > 0) {
					result.put(entry.getKey(), entry.getValue());
					executorCount--;
					distribution.put(entry.getValue().size(), executorCount);
				}
			}
			for(WorkerSlot slot : result.keySet())
				existingSlots.remove(slot);
			return existingSlots.keySet();
		}
		return null;
	}

	@Override
	public void prepare(Map conf) {
		// TODO Auto-generated method stub
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		// TODO Auto-generated method stub
		List<TopologyDetails> needsSchedulingTopologies = cluster.needsSchedulingTopologies(topologies);
		for(TopologyDetails details : needsSchedulingTopologies) {
			List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
			Set<ExecutorDetails> allExecutors = (Set<ExecutorDetails>) details.getExecutors();
			Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = EvenScheduler.getAliveAssignedWorkerSlotExecutors(cluster, details.getId());
			Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
			for(List<ExecutorDetails> cache : aliveAssigned.values()) {
				aliveExecutors.addAll(cache);
			}
			Set<WorkerSlot> canReassignSlots = slotsCanReassign(cluster, aliveAssigned.keySet());
			int totalSlotsToUse = Math.min(details.getNumWorkers(), canReassignSlots.size() + availableSlots.size());
			Set<WorkerSlot> badSlot = null;
			if(aliveAssigned.size() < totalSlotsToUse || !allExecutors.equals(aliveExecutors))
				badSlot = badSlots(aliveAssigned, allExecutors.size(), totalSlotsToUse);
			if(badSlot != null)
				cluster.freeSlots(badSlot);
			Map<String, TopologyDetails> topologiesCache = new HashMap<String, TopologyDetails>();
			topologiesCache.put(details.getId(), details);
			EvenScheduler.scheduleTopologiesEvenly(new Topologies(topologiesCache), cluster);
		}
	}

}
