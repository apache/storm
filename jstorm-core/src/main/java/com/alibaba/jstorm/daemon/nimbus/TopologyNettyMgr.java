package com.alibaba.jstorm.daemon.nimbus;

import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.generated.InvalidTopologyException;

public class TopologyNettyMgr {
	private static Logger LOG = LoggerFactory.getLogger(TopologyNettyMgr.class);
	private boolean defaultEnable = true;
	private Map nimbusConf;
	private ConcurrentHashMap<String, Boolean> setting = new ConcurrentHashMap<String, Boolean>();
	private static final int WORKER_DISABLE_THREADHOLD = 200;
	
	public TopologyNettyMgr(Map conf) {
		nimbusConf = conf;
		
		Boolean isEnable = ConfigExtension.isEnableTopologyNettyMetrics(conf);
		if (isEnable != null) {
			defaultEnable = isEnable;
		}
		
		LOG.info("Default netty metrics setting is " + defaultEnable);
	}
	
	protected boolean getTopology(Map conf) {
		Boolean isEnable = ConfigExtension.isEnableTopologyNettyMetrics(conf);
		if (isEnable != null) {
			return isEnable;
		}
		
		int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 1);
		if (workerNum <=  WORKER_DISABLE_THREADHOLD) {
			isEnable = Boolean.TRUE;
		}else {
			isEnable = Boolean.FALSE;
		}
		
		return isEnable;
	}
	
	public boolean getTopology(String topologyId) {
		try {
			String topologyName = Common.topologyIdToName(topologyId);
			
			Boolean isEnable = setting.get(topologyName);
			if (isEnable != null) {
				return isEnable;
			}
			
			Map topologyConf =
	                StormConfig.read_nimbus_topology_conf(nimbusConf, topologyId);
			
		    isEnable = getTopology(topologyConf);
			setting.put(topologyName, isEnable);
			LOG.info("{} netty metrics setting is {}", topologyName, isEnable);
			return isEnable;
		
		}catch(Exception e) {
			LOG.info("Failed to get {} netty metrics setting ", topologyId);
			return defaultEnable;
		}
		
		
	}
	
	public void setTopology(Map conf) {
		String topologyName = (String)conf.get(Config.TOPOLOGY_NAME);
		if (topologyName == null) {
			LOG.info("No topologyName setting");
			return ;
		}
		
		boolean isEnable = getTopology(conf);
		
		setting.put(topologyName, isEnable);
		
		LOG.info("{} netty metrics setting is {}", topologyName, isEnable);
		return ;
		
	}
	
	public void rmTopology(String topologyId) {
		String topologyName;
		try {
			topologyName = Common.topologyIdToName(topologyId);
			setting.remove(topologyName);
			LOG.info("Remove {} netty metrics setting ", topologyName);
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			
		}
		
	}

}
