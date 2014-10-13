package backtype.storm;

import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

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

import com.alibaba.fastjson.JSON;

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
			StormTopology topology) {
		submitTopologyWithOpts(topologyName, conf, topology, null);
	}

	@Override
	public void submitTopologyWithOpts(String topologyName, Map conf,
			StormTopology topology, SubmitOptions submitOpts){
		// TODO Auto-generated method stub
		if (!Utils.isValidConf(conf))
			throw new RuntimeException("Topology conf is not json-serializable");
		try {
			if (submitOpts == null) {
				state.getNimbus().submitTopology(topologyName, null,
						JSON.toJSONString(conf), topology);
			}else {
				state.getNimbus().submitTopologyWithOpts(topologyName, null,
						JSON.toJSONString(conf), topology, submitOpts);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to submit topology " + topologyName, e);
			throw new RuntimeException(e);
		} 
	}

	@Override
	public void killTopology(String topologyName) {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().killTopology(topologyName);
		} catch (Exception e) {
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
			throw new RuntimeException(e);
		}
	}

	@Override
	public void activate(String topologyName)  {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().activate(topologyName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("fail to activate " + topologyName, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deactivate(String topologyName) {
		// TODO Auto-generated method stub
		try {
			state.getNimbus().deactivate(topologyName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("fail to deactivate " + topologyName, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rebalance(String name, RebalanceOptions options){
		// TODO Auto-generated method stub
		try {
			state.getNimbus().rebalance(name, options);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("fail to rebalance " + name, e);
			throw new RuntimeException(e);
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
		}  catch (Exception e) {
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
			if(localCluster != null) {
				localCluster.shutdown();
			}	
		}
	}

}
