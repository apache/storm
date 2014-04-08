package com.alibaba.jstorm.local;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

import backtype.storm.ILocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

public class LocalCluster implements ILocalCluster {

	public static Logger LOG = Logger.getLogger(LocalCluster.class);

	private LocalClusterMap state;

	public LocalCluster() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		this.state = LocalUtils.prepareLocalCluster();
		if (this.state == null)
			throw new RuntimeException("prepareLocalCluster error");
	}

	@Override
	public void submitTopology(String topologyName, Map conf,
			StormTopology topology) throws AlreadyAliveException,
			InvalidTopologyException {
		// TODO Auto-generated method stub
		if (!Utils.isValidConf(conf))
			throw new RuntimeException("Topology conf is not json-serializable");
		try {
			state.getNimbus().submitTopology(topologyName, null,
					JSONValue.toJSONString(conf), topology);
		} catch (TopologyAssignException e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to submit topology " + topologyName, e);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to submit topology " + topologyName, e);
		}
	}

	@Override
	public void submitTopologyWithOpts(String topologyName, Map conf,
			StormTopology topology, SubmitOptions submitOpts)
			throws AlreadyAliveException, InvalidTopologyException {
		// TODO Auto-generated method stub
		if (!Utils.isValidConf(conf))
			throw new RuntimeException("Topology conf is not json-serializable");
		try {
			state.getNimbus().submitTopologyWithOpts(topologyName, null,
					JSONValue.toJSONString(conf), topology, submitOpts);
		} catch (TopologyAssignException e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to submit topology " + topologyName, e);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to submit topology " + topologyName, e);
		}
	}

	@Override
	public void killTopology(String topologyName) throws NotAliveException {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().killTopology(topologyName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to kill Topology " + topologyName, e);
		}
	}

	@Override
	public void killTopologyWithOpts(String name, KillOptions options)
			throws NotAliveException {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().killTopologyWithOpts(name, options);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to kill Topology " + name, e);
		}
	}

	@Override
	public void activate(String topologyName) throws NotAliveException {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().activate(topologyName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to activate " + topologyName, e);
		}
	}

	@Override
	public void deactivate(String topologyName) throws NotAliveException {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().deactivate(topologyName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to deactivate " + topologyName, e);
		}
	}

	@Override
	public void rebalance(String name, RebalanceOptions options)
			throws NotAliveException {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().rebalance(name, options);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to rebalance " + name, e);
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to rebalance " + name, e);
		}
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		this.state.clean();
	}

	@Override
	public String getTopologyConf(String id) {
		// TODO Auto-generated method stub
		try {
			return state.getNimbus().getTopologyConf(id);
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology Conf of topologId: " + id, e);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology Conf of topologId: " + id, e);
		}
		return null;
	}

	@Override
	public StormTopology getTopology(String id) {
		// TODO Auto-generated method stub
		try {
			return state.getNimbus().getTopology(id);
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology of topologId: " + id, e);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology of topologId: " + id, e);
		}
		return null;
	}

	@Override
	public ClusterSummary getClusterInfo() {
		// TODO Auto-generated method stub
		try {
			return state.getNimbus().getClusterInfo();
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get cluster info", e);
		}
		return null;
	}

	@Override
	public TopologyInfo getTopologyInfo(String id) {
		// TODO Auto-generated method stub
		try {
			return state.getNimbus().getTopologyInfo(id);
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology info of topologyId: " + id, e);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error("fail to get topology info of topologyId: " + id, e);
		}
		return null;
	}

	/***
	 * You should use getLocalClusterMap() to instead.This function will always
	 * return null
	 * */
	@Deprecated
	@Override
	public Map getState() {
		// TODO Auto-generated method stub
		return null;
	}

	public LocalClusterMap getLocalClusterMap() {
		return state;
	}

	public static void main(String[] args) throws Exception {
		LocalCluster localCluster = null;
		try {
			localCluster = new LocalCluster();
		} finally {
			localCluster.shutdown();
		}
	}

}
