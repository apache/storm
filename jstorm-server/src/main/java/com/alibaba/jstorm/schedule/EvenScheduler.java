package com.alibaba.jstorm.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.daemon.nimbus.NimbusServer;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

@Deprecated
public class EvenScheduler implements IScheduler {

	private static final Logger LOG = Logger.getLogger(EvenScheduler.class);

	public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(
			Cluster cluster, String topologyId) {
		SchedulerAssignment existingAssignment = cluster
				.getAssignmentById(topologyId);
		Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
		if (existingAssignment != null) {
			executorToSlot = existingAssignment.getExecutorToSlot();
		}
		Map<WorkerSlot, List<ExecutorDetails>> result = new HashMap<WorkerSlot, List<ExecutorDetails>>();
		if (executorToSlot != null) {
			for (Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot
					.entrySet()) {
				List<ExecutorDetails> cache = result.get(entry.getValue());
				if (cache == null) {
					cache = new ArrayList<ExecutorDetails>();
					result.put(entry.getValue(), cache);
				}
				cache.add(entry.getKey());
			}
		}
		return result;
	}

	private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(
			TopologyDetails topology, Cluster cluster) {
		List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
		Set<ExecutorDetails> allExecutors = (Set<ExecutorDetails>) topology
				.getExecutors();
		Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(
				cluster, topology.getId());
		int totalSlotsToUse = Math.min(topology.getNumWorkers(),
				availableSlots.size() + aliveAssigned.size());
		Collections.sort(availableSlots, new Comparator<WorkerSlot>() {

			@Override
			public int compare(WorkerSlot o1, WorkerSlot o2) {
				// TODO Auto-generated method stub
				if (o1.getPort() > o2.getPort())
					return 1;
				else if (o1.getPort() == o2.getPort())
					return 0;
				else
					return -1;
			}

		});
		List<WorkerSlot> reassignSlots = availableSlots.subList(0,
				totalSlotsToUse - aliveAssigned.size());
		Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
		for (List<ExecutorDetails> cache : aliveAssigned.values()) {
			aliveExecutors.addAll(cache);
		}
		for (ExecutorDetails details : allExecutors) {
			if (aliveExecutors.contains(details))
				allExecutors.remove(details);
		}
		Map<ExecutorDetails, WorkerSlot> result = new HashMap<ExecutorDetails, WorkerSlot>();
		int num = 0;
		int length = reassignSlots.size();
		for (ExecutorDetails details : allExecutors) {
			int key = num % length;
			result.put(details, reassignSlots.get(key));
			num++;
		}
		if (result.size() != 0) {
			LOG.info("Available slot:");
			for (WorkerSlot slot : availableSlots)
				LOG.info(slot.toString());
		}
		return result;
	}

	public static void scheduleTopologiesEvenly(Topologies topologies,
			Cluster cluster) {
		List<TopologyDetails> needsSchedulingTopologies = cluster
				.needsSchedulingTopologies(topologies);
		for (TopologyDetails topology : needsSchedulingTopologies) {
			String topologyId = topology.getId();
			Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(
					topology, cluster);
			Map<WorkerSlot, List<ExecutorDetails>> result = new HashMap<WorkerSlot, List<ExecutorDetails>>();
			for (Entry<ExecutorDetails, WorkerSlot> entry : newAssignment
					.entrySet()) {
				List<ExecutorDetails> cache = result.get(entry.getValue());
				if (cache == null) {
					cache = new ArrayList<ExecutorDetails>();
					result.put(entry.getValue(), cache);
				}
				cache.add(entry.getKey());
			}
			for (Entry<WorkerSlot, List<ExecutorDetails>> entry : result
					.entrySet()) {
				cluster.assign(entry.getKey(), topologyId, entry.getValue());
			}
		}
	}

	@Override
	public void prepare(Map conf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		// TODO Auto-generated method stub
		scheduleTopologiesEvenly(topologies, cluster);
	}

}
