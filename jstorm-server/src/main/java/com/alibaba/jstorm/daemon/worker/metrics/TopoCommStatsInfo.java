package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.stats.CommonStatsData;

public class TopoCommStatsInfo {
	private static final Logger LOG = Logger.getLogger(TopoCommStatsInfo.class);
	
	private String topologyId;
	private String topologyName;
	// Map<ComponentId, List<TaskId, TaskHeartbeat>>
	private Map<String, Map<String, TaskHeartbeat>> spoutHbMap;
	private Map<String, Map<String, TaskHeartbeat>> boltHbMap;
	
	//*********** Statistic data ****************//
	// Topology Data
	private CommStatsData topoStatsData;
	// Spout Data
	private Map<String, CommStatsData> spoutStatsDataMap;
	// Bolt Data
	private Map<String, CommStatsData> boltStatsDataMap;
	
	public TopoCommStatsInfo(String topologyId, String topologyName) {
		this.topologyId = topologyId;
		this.topologyName = topologyName;
		topoStatsData = new CommStatsData();
		spoutHbMap = new HashMap<String, Map<String, TaskHeartbeat>>();
		boltHbMap = new HashMap<String, Map<String, TaskHeartbeat>>();
		spoutStatsDataMap = new HashMap<String, CommStatsData>();
		boltStatsDataMap = new HashMap<String, CommStatsData>();
	}
		
	public String getTopoId() {
		return topologyId;
	}
	
	public String getTopoName() {
		return topologyName;
	}
		
	public Map<String, Map<String, TaskHeartbeat>> getSpoutList() {
		return spoutHbMap;
	}
		
	public Map<String, Map<String, TaskHeartbeat>> getBoltList() {
		return boltHbMap;
	}
	
	public CommStatsData getTopoStatsData() {
		return topoStatsData;
	}
	
	public Map<String, CommStatsData> getSpoutStatsData() {
		return spoutStatsDataMap;
	}
	
	public Map<String, CommStatsData> getBoltStatsData() {
		return boltStatsDataMap;
	}
		
	public void addToSpoutList(String componentId, String taskId, TaskHeartbeat taskHb) {
		Map<String, TaskHeartbeat> taskMap = spoutHbMap.get(componentId);
		if (taskMap == null) {
			taskMap = new HashMap<String, TaskHeartbeat>();
			spoutHbMap.put(componentId, taskMap);
		}
		taskMap.put(taskId, taskHb);
	}
		
	public void addToBoltList(String componentId, String taskId, TaskHeartbeat taskHb) {
		Map<String, TaskHeartbeat> taskMap = boltHbMap.get(componentId);
		if (taskMap == null) {
			taskMap = new HashMap<String, TaskHeartbeat>();
			boltHbMap.put(componentId, taskMap);
		}
		taskMap.put(taskId, taskHb);
	}
		
	public void buildTopoStatsData() {
		topoStatsData.resetData();
		Double latency = 0.0;
		for (Entry<String, CommStatsData> spoutEntry : spoutStatsDataMap.entrySet()) {
			CommStatsData statsData = spoutEntry.getValue();
			topoStatsData.updateSendTps(statsData.getSendTps());
			topoStatsData.updateRecvTps(statsData.getRecvTps());
			topoStatsData.updateFailed(statsData.getFailed());
			latency += statsData.getLatency();
		}
		latency = latency/(spoutStatsDataMap.size());
		topoStatsData.updateLatency(latency);
		
		for (Entry<String, CommStatsData> boltEntry : boltStatsDataMap.entrySet()) {
			CommStatsData statsData = boltEntry.getValue();
			topoStatsData.updateSendTps(statsData.getSendTps());
			topoStatsData.updateRecvTps(statsData.getRecvTps());
			topoStatsData.updateFailed(statsData.getFailed());
		}
	}
		
	public void buildSpoutStatsData() {
		updateStatsData(spoutHbMap, spoutStatsDataMap);
	}
		
	public void buildBoltStatsData() {
		updateStatsData(boltHbMap, boltStatsDataMap);
	}
	
	public void updateStatsData(Map<String, Map<String, TaskHeartbeat>> HbMap, Map<String, CommStatsData> statsDataMap) {
		for (Entry<String, Map<String, TaskHeartbeat>> Entry : HbMap.entrySet()) {
			String componentId = Entry.getKey();
			Map<String, TaskHeartbeat> compList = Entry.getValue();
			
			CommStatsData comStatsData = statsDataMap.get(componentId);
			if (comStatsData == null) {
				comStatsData = new CommStatsData();
				statsDataMap.put(componentId, comStatsData);
			}
			comStatsData.resetData();
			
			for (Entry<String, TaskHeartbeat> compEntry : compList.entrySet()) {
				TaskHeartbeat taskHb = compEntry.getValue();
				CommonStatsData statsData = taskHb.getStats();
				comStatsData.updateStatsData(statsData);
			}
			double avgLatency = (comStatsData.getLatency())/(compList.size());
			comStatsData.updateLatency(avgLatency);
		}
	}
		
		
	public class CommStatsData {
		private static final String TOPOLOGYNAME = "TopologyName";
		private static final String COMPONTENT= "Component";
		private static final String SEND_TPS = "send_tps";
		private static final String RECV_TPS = "recv_tps";
		private static final String FAILED = "failed";
		private static final String LATENCY = "process_latency";
		
		private Double sendTps;
		private Double recvTps;
		private Long failed;
		private Double latency;
		
		public CommStatsData() {
			resetData();
		}
		
		public Double getSendTps() {
			return sendTps;
		}
		
		public Double getRecvTps() {
			return recvTps;
		}
		
		public Long getFailed() {
			return failed;
		}
		
		public Double getLatency() {
			return latency;
		}
		
		public void updateSendTps(Double tps) {
			sendTps += tps;
		}
		
		public void updateRecvTps(Double tps) {
			recvTps += tps;
		}
		
		public void updateFailed(Long fail) {
			failed += fail;
		}
		
		public void updateLatency(Double latency) {
			this.latency = latency;
		}
		
		public void updateStatsData(CommonStatsData commStatsData) {
			sendTps += commStatsData.get_total_send_tps();
			recvTps += commStatsData.get_total_recv_tps();
			failed += commStatsData.get_total_failed();
			latency += commStatsData.get_avg_latency();
		}
		
		public void updateStatsData(CommStatsData commStatsData) {
			sendTps += commStatsData.getSendTps();
			recvTps += commStatsData.getRecvTps();
			failed += commStatsData.getFailed();
			latency += commStatsData.getLatency();
		}
		
		public void resetData() {
			sendTps = 0.0;
			recvTps = 0.0;
			failed = 0l;
			latency = 0.0;
		}
		
		public Map<String, Object> convertToKVMap(String topoloygName,String componentId) {
			Map<String, Object> ret = new HashMap<String, Object>();  
			ret.put(TOPOLOGYNAME, topoloygName);
			ret.put( COMPONTENT, componentId);
			ret.put(SEND_TPS, sendTps);
			ret.put( RECV_TPS, recvTps);
			ret.put(FAILED, failed);
			ret.put(LATENCY, latency);
			
			return ret;
		}
		
		public void printValue() {
			LOG.info("send_tps: " + sendTps);
			LOG.info("recv_tps: " + recvTps);
			LOG.info("failed: " + failed);
			LOG.info("latency: " + latency);
		}
	}
}
	
	