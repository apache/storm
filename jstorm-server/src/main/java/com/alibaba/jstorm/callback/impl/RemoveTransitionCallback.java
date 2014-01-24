package com.alibaba.jstorm.callback.impl;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;

/**
 * Remove topology /ZK-DIR/topology data
 * 
 * remove this ZK node will trigger watch on this topology
 * 
 * And Monitor thread every 10 seconds will clean these disappear topology
 * 
 */
public class RemoveTransitionCallback extends BaseCallback {

	private static Logger LOG = Logger
			.getLogger(RemoveTransitionCallback.class);

	private NimbusData data;
	private String topologyid;

	public RemoveTransitionCallback(NimbusData data, String topologyid) {
		this.data = data;
		this.topologyid = topologyid;
	}

	@Override
	public <T> Object execute(T... args) {
		LOG.info("Begin to remove topology: " + topologyid);
		boolean locked = false;
		try {
			
			StormBase stormBase = data.getStormClusterState()
					.storm_base(topologyid, null);
			if (stormBase == null) {
				LOG.info("Topology " + topologyid + " has been removed ");
				return null;
			}
			String topologyName = stormBase.getStormName();
			String group = stormBase.getGroup();
			
			data.getStormClusterState().remove_storm(topologyid);
			LOG.info("Successfully removed ZK items topology: " + topologyid);
			
			data.getFlushGroupFileLock().lock();
			locked = true;
			
			NimbusUtils.releaseGroupResource(data, topologyName, group);
			
			data.getFlushGroupFileLock().unlock();
			locked = false;
			
			LOG.info("Release " + topologyName + " resource from Group Pool");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.warn("Failed to remove StormBase " + topologyid + " from ZK", e);
		} finally {
			if (locked == true) {
				data.getFlushGroupFileLock().unlock();
			}
		}
		return null;
	}

}
