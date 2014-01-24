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
 * The action when nimbus receive rebalance command. Rebalance command is only
 * valid when current status is active
 * 
 * 1. change current topology status as rebalancing 2. do_rebalance action after
 * 2 * TIMEOUT seconds
 * 
 * @author Lixin/Longda
 * 
 */
public class RebalanceTransitionCallback extends BaseCallback {

	private static Logger LOG = Logger
			.getLogger(RebalanceTransitionCallback.class);

	private NimbusData data;
	private String topologyid;
	private StormStatus oldStatus;

	public RebalanceTransitionCallback(NimbusData data, String topologyid,
			StormStatus status) {
		this.data = data;
		this.topologyid = topologyid;
		this.oldStatus = status;
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

		LOG.info("Delaying event " + StatusType.do_rebalance.getStatus()
				+ " for " + delaySecs + " secs for " + topologyid);

		data.getScheduExec().schedule(
				new DelayEventRunnable(data, topologyid,
						StatusType.do_rebalance), delaySecs, TimeUnit.SECONDS);

		return new StormStatus(delaySecs, StatusType.rebalancing, oldStatus);
	}

}
