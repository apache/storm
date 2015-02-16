package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.stats.CommonStatsData;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.UserDefMetricData;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

public class UploadMetricFromZK implements Runnable {
    
    private static final Logger LOG = Logger.getLogger(UploadMetricFromZK.class);

    private NimbusData data;
    private StormClusterState clusterState;
    
    private MetricSendClient client;
    
    private Map<String, TopoCommStatsInfo> topologyMap;
    
    public UploadMetricFromZK(NimbusData data, MetricSendClient client) {
        this.data = data;
        this.client = client;
        clusterState = data.getStormClusterState();
        topologyMap = new HashMap<String, TopoCommStatsInfo>();
    }

    @Override
    public void run() {
        uploadCommStats();
        uploadUseDefMetric(clusterState); 
    }

	// remove obsolete topology
	private boolean rmObsoleteTopo() {
		boolean ret = true;
		List<String> obsoleteTopos = new ArrayList<String>();
		try {
		    List<String> topologys = clusterState.active_storms();
		    
		    for (Entry<String, TopoCommStatsInfo> entry : topologyMap.entrySet()) {
		    	if (topologys.contains(entry.getKey()) == false) {
		    		obsoleteTopos.add(entry.getKey());
		    	}
		    }
		    
		    for (String topo : obsoleteTopos) {
		    	topologyMap.remove(topo);
		    }
		} catch (Exception e) {
			LOG.warn("Faild to update topology list.", e);
			ret = false;
		}
		
		return ret;
	}

    private void uploadCommStats() {
    	// Remove obsolete topology firstly. new topology will be
        // added when uploading the common statistic data
        rmObsoleteTopo();
        
        List<Map<String,Object>> listMapMsg=new ArrayList<Map<String,Object>>();
    	
        try {
            TopoCommStatsInfo ret;
            List<String> topologys = clusterState.heartbeat_storms();

            for (String topologyId : topologys) {
                if (topologyMap.containsKey(topologyId) == false) {
                    StormBase base = clusterState.storm_base(topologyId, null);
                    if (base == null) {
                    	topologyMap.remove(topologyId);
                    	continue;
                    } else {
                    	topologyMap.put(topologyId, new TopoCommStatsInfo(topologyId, base.getStormName()));
                    }
                }
                // Get common statistic data from taskbeats in ZK for a topology
                ret = getCommStatsData(topologyId);	

                if (ret != null) {
                    // Build topology, spout and bolt statis data from the 
                    // statis data of all tasks 
                    buildCommStatsData(ret);
                    // Build statistic data message of remote monitor server
                    buildComonSendMsg(ret,listMapMsg);
                   
                }
            }
            
            if(listMapMsg.size() > 0) {
                // Send statistic data to remote monitor server
                sendCommStatsData(listMapMsg);
            }
        } catch (Exception e) {
            LOG.warn("Failed to upload comm statistic data to Alimonitor.", e);
        }
    }

	public void uploadUseDefMetric(StormClusterState clusterState) {
	    try {	
		    List<String> active_topologys = clusterState.active_storms();
		    if (active_topologys == null) {
			    return;
		    }
		
		    Map<String, Object> totalMsg = new HashMap<String, Object>();
		    
		    for (String topologyId : active_topologys) {
		        MetricKVMsg topologyMetricMsg = MetricKVMsg.getMetricKVMsg(topologyId, clusterState);
			    Map<String, Object> ret = topologyMetricMsg.convertToKVMap();
			    if(ret.size() >0) totalMsg.putAll(ret);
		    }
		    
		    if(totalMsg.size() > 0) {
		    	// For Alimonitor Client only
		    	if (client instanceof AlimonitorClient) {
		    	    ((AlimonitorClient) client).setMonitorName(
		    	    		ConfigExtension.getAlmonUserMetricName(data.getConf()));
		    	    ((AlimonitorClient) client).setCollectionFlag(0);
				    ((AlimonitorClient) client).setErrorInfo("");
		    	}
		        client.send(totalMsg);
		    }
	    } catch (Exception e) {
	    	LOG.warn("Failed to upload user define metric data", e);
        }
	}
	
	public void clean() {

	}
	
	private TopoCommStatsInfo getCommStatsData(String topologyId) {
		try
		{   
			String taskId;
			String componentId;
		    TaskHeartbeat taskHb;
		    
		    TopoCommStatsInfo commStatsInfo = topologyMap.get(topologyId);
		    if (commStatsInfo == null) {LOG.warn("commStatsInfo is null, topoId=" + topologyId);}
		    
		    Map<String, TaskHeartbeat> heartbeats = clusterState.task_heartbeat(topologyId);
		    if (heartbeats == null || heartbeats.size() == 0) return null;
		    
		    for (Entry<String, TaskHeartbeat> entry : heartbeats.entrySet()) {
		    	taskId = entry.getKey();
		        taskHb = entry.getValue();
		        
		        TaskInfo taskInfo = clusterState.task_info(topologyId, Integer.parseInt(taskId));
		        if (taskInfo == null ) {
		        	LOG.warn("Task information can not be found in ZK for task-" + taskId);
		        	continue;
		        }
		        componentId = taskInfo.getComponentId();
		        
		        //update taskHb into the corresponding component map
		        if (taskHb.getComponentType().equals("spout")) {
		        	commStatsInfo.addToSpoutList(componentId, taskId, taskHb);
		        } else {
		        	commStatsInfo.addToBoltList(componentId, taskId, taskHb);
		        }
		    }
		    
		    return commStatsInfo;

		} catch (Exception e) {
			LOG.warn("getCommStatsData, failed to read data from ZK.", e);
			return null;
		}
	}
	
	private void buildCommStatsData(TopoCommStatsInfo commStatsInfo) {
		commStatsInfo.buildBoltStatsData();
		commStatsInfo.buildSpoutStatsData();
		commStatsInfo.buildTopoStatsData();
	}
	
    private void sendCommStatsData(List<Map<String,Object>> listMapMsg) {
			
		try {
			// For Alimonitor Client only
			if (client instanceof AlimonitorClient) {
			    ((AlimonitorClient) client).setMonitorName(
			    		ConfigExtension.getAlmonTopoMetricName(data.getConf()));
			    ((AlimonitorClient) client).setCollectionFlag(0);
			    ((AlimonitorClient) client).setErrorInfo("");
			}
		    client.send(listMapMsg);
		} catch (Exception e) {
			LOG.warn("Error when sending common statistic data.", e);
		}
	}
    
    private void  buildComonSendMsg(TopoCommStatsInfo commStatsInfo,List<Map<String,Object>> listMapMsg) {
		String topoloygName = commStatsInfo.getTopoName();
		
		Map<String, Object> jsonMsg;
		
		try {
		    //build topology statistic data
		    TopoCommStatsInfo.CommStatsData topoStatsData = commStatsInfo.getTopoStatsData();
		    jsonMsg = topoStatsData.convertToKVMap(topoloygName,topoloygName);
		    	listMapMsg.add(jsonMsg);
		    //build spout statistic data
		    Map<String, TopoCommStatsInfo.CommStatsData> spoutStatsData = commStatsInfo.getSpoutStatsData();
		    for (Entry<String, TopoCommStatsInfo.CommStatsData> entry : spoutStatsData.entrySet()) {
			    String componentId = entry.getKey();
			    jsonMsg = entry.getValue().convertToKVMap(topoloygName,componentId);
			    listMapMsg.add(jsonMsg);
		    }
		
		    //build bolt statistic data
		    Map<String, TopoCommStatsInfo.CommStatsData> boltStatsData = commStatsInfo.getBoltStatsData();
		    for (Entry<String, TopoCommStatsInfo.CommStatsData> entry : boltStatsData.entrySet()) {
			    String componentId = entry.getKey();
			    jsonMsg = entry.getValue().convertToKVMap(topoloygName,componentId);
			    listMapMsg.add(jsonMsg);
		    }
		} catch (Exception e) {
			LOG.warn("Error when bulding common statistic data message.", e);
		}
	}
    

}