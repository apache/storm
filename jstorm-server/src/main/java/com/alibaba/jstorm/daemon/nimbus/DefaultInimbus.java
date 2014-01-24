package com.alibaba.jstorm.daemon.nimbus;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;

public class DefaultInimbus implements INimbus {

	@Override
	public void prepare(Map stormConf, String schedulerLocalDir) {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<WorkerSlot> allSlotsAvailableForScheduling(
			Collection<SupervisorDetails> existingSupervisors,
			Topologies topologies, Set<String> topologiesMissingAssignments) {
		// TODO Auto-generated method stub
		Collection<WorkerSlot> result = new HashSet<WorkerSlot>();
		for (SupervisorDetails detail : existingSupervisors) {
			for (Integer port : detail.getAllPorts())
				result.add(new WorkerSlot(detail.getId(), port));
		}
		return result;
	}

	@Override
	public void assignSlots(Topologies topologies,
			Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getHostName(
			Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IScheduler getForcedScheduler() {
		// TODO Auto-generated method stub
		return null;
	}

}
