package com.alibaba.jstorm.callback.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.DelayEventRunnable;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * 
 * The action when nimbus receive kill command 1. set the topology status as
 * killed 2. wait 2 * Timeout seconds later, do removing topology from ZK
 * 
 * @author Li xin/Longda
 */
public class KillTransitionCallback extends BaseCallback {

	private static Logger LOG = Logger.getLogger(KillTransitionCallback.class);

	public static final int DEFAULT_DELAY_SECONDS = 60;

	private NimbusData data;
	private String topologyid;
	private StormStatus oldStatus;

	public KillTransitionCallback(NimbusData data, String topologyid) {
		this.data = data;
		this.topologyid = topologyid;
	}

	@Override
	public <T> Object execute(T... args) {
		Integer delaySecs = KillTransitionCallback.DEFAULT_DELAY_SECONDS;
		if (args == null || args.length == 0 || args[0] == null) {
			Map<?, ?> map = null;
			try {

				map = StormConfig.read_nimbus_topology_conf(data.getConf(),
						topologyid);
				delaySecs = JStormUtils.parseInt(map
						.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
				if (delaySecs != null) {
					delaySecs = delaySecs * 2;
				} else {
					LOG.info("Fail to get TOPOLOGY_MESSAGE_TIMEOUT_SECS " + map);
				}
			} catch (Exception e) {
				LOG.info("Failed to get topology configuration " + topologyid);
			}

		} else {
			delaySecs = Integer.valueOf(String.valueOf(args[0]));
		}

		if (delaySecs == null || delaySecs <= 0) {
			delaySecs = KillTransitionCallback.DEFAULT_DELAY_SECONDS;
		}
		LOG.info("Delaying event " + StatusType.remove.getStatus() + " for "
				+ delaySecs + " secs for " + topologyid);

		data.getScheduExec().schedule(
				new DelayEventRunnable(data, topologyid, StatusType.remove),
				delaySecs, TimeUnit.SECONDS);

		return new StormStatus(delaySecs, StatusType.killed);
	}

}
