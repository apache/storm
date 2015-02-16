package com.alibaba.jstorm.daemon.nimbus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.callback.impl.ActiveTransitionCallback;
import com.alibaba.jstorm.callback.impl.DoRebalanceTransitionCallback;
import com.alibaba.jstorm.callback.impl.InactiveTransitionCallback;
import com.alibaba.jstorm.callback.impl.KillTransitionCallback;
import com.alibaba.jstorm.callback.impl.ReassignTransitionCallback;
import com.alibaba.jstorm.callback.impl.RebalanceTransitionCallback;
import com.alibaba.jstorm.callback.impl.RemoveTransitionCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormStatus;

/**
 * Status changing
 * 
 * @author version1: lixin version2: Longda
 * 
 * 
 * 
 */
public class StatusTransition {

	private final static Logger LOG = Logger.getLogger(StatusTransition.class);

	private NimbusData data;

	private Map<String, Object> topologyLocks = new ConcurrentHashMap<String, Object>();

	public StatusTransition(NimbusData data) {
		this.data = data;

	}

	public <T> void transition(String topologyid, boolean errorOnNoTransition,
			StatusType changeStatus, T... args) throws Exception {
		// lock outside
		Object lock = topologyLocks.get(topologyid);
		if (lock == null) {
			lock = new Object();
			topologyLocks.put(topologyid, lock);
		}
		
		if (data.getIsShutdown().get() == true) {
			LOG.info("Nimbus is in shutdown, skip this event " +
					topologyid + ":" +changeStatus);
			return ;
		}

		synchronized (lock) {
			transitionLock(topologyid, errorOnNoTransition, changeStatus, args);

			// update the lock times
			topologyLocks.put(topologyid, lock);
		}
	}

	/**
	 * Changing status
	 * 
	 * @param topologyId
	 * @param errorOnNTransition
	 *            if it is true, failure will throw exception
	 * @param args
	 *            -- will be used in the status changing callback
	 * 
	 */
	public <T> void transitionLock(String topologyid,
			boolean errorOnNoTransition, StatusType changeStatus, T... args)
			throws Exception {

		// get ZK's topology node's data, which is StormBase
		StormBase stormbase = data.getStormClusterState().storm_base(
				topologyid, null);
		if (stormbase == null) {

			LOG.error("Cannot apply event changing status "
					+ changeStatus.getStatus() + " to " + topologyid
					+ " because failed to get StormBase from ZK");
			return;
		}

		StormStatus currentStatus = stormbase.getStatus();
		if (currentStatus == null) {
			LOG.error("Cannot apply event changing status "
					+ changeStatus.getStatus() + " to " + topologyid
					+ " because topologyStatus is null in ZK");
			return;
		}

		// <currentStatus, Map<changingStatus, callback>>
		Map<StatusType, Map<StatusType, Callback>> callbackMap = stateTransitions(
				topologyid, currentStatus);

		// get current changingCallbacks
		Map<StatusType, Callback> changingCallbacks = callbackMap
				.get(currentStatus.getStatusType());

		if (changingCallbacks == null
				|| changingCallbacks.containsKey(changeStatus) == false
				|| changingCallbacks.get(changeStatus) == null) {
			String msg = "No transition for event: changing status:"
					+ changeStatus.getStatus() + ", current status: "
					+ currentStatus.getStatusType() + " topology-id: "
					+ topologyid;
			LOG.info(msg);
			if (errorOnNoTransition) {
				throw new RuntimeException(msg);
			}
			return;
		}

		Callback callback = changingCallbacks.get(changeStatus);

		Object obj = callback.execute(args);
		if (obj != null && obj instanceof StormStatus) {
			StormStatus newStatus = (StormStatus) obj;
			// update status to ZK
			data.getStormClusterState().update_storm(topologyid, newStatus);
			LOG.info("Successfully updated " + topologyid + " as status "
					+ newStatus);
		}

		LOG.info("Successfully apply event changing status "
				+ changeStatus.getStatus() + " to " + topologyid);
		return;

	}

	/**
	 * generate status changing map
	 * 
	 * 
	 * 
	 * @param topologyid
	 * @param status
	 * @return
	 * 
	 *         Map<StatusType, Map<StatusType, Callback>> means
	 *         Map<currentStatus, Map<changingStatus, Callback>>
	 */

	private Map<StatusType, Map<StatusType, Callback>> stateTransitions(
			String topologyid, StormStatus currentStatus) {

		/**
		 * 
		 * 1. Status: this status will be stored in ZK
		 * killed/inactive/active/rebalancing 2. action:
		 * 
		 * monitor -- every Config.NIMBUS_MONITOR_FREQ_SECS seconds will trigger
		 * this only valid when current status is active inactivate -- client
		 * will trigger this action, only valid when current status is active
		 * activate -- client will trigger this action only valid when current
		 * status is inactive startup -- when nimbus startup, it will trigger
		 * this action only valid when current status is killed/rebalancing kill
		 * -- client kill topology will trigger this action, only valid when
		 * current status is active/inactive/killed remove -- 30 seconds after
		 * client submit kill command, it will do this action, only valid when
		 * current status is killed rebalance -- client submit rebalance
		 * command, only valid when current status is active/deactive
		 * do_rebalance -- 30 seconds after client submit rebalance command, it
		 * will do this action, only valid when current status is rebalance
		 */

		Map<StatusType, Map<StatusType, Callback>> rtn = new HashMap<StatusType, Map<StatusType, Callback>>();

		// current status is active
		Map<StatusType, Callback> activeMap = new HashMap<StatusType, Callback>();
		activeMap.put(StatusType.monitor, new ReassignTransitionCallback(data,
				topologyid));
		activeMap.put(StatusType.inactivate, new InactiveTransitionCallback());
		activeMap.put(StatusType.startup, null);
		activeMap.put(StatusType.activate, null);
		activeMap.put(StatusType.kill, new KillTransitionCallback(data,
				topologyid));
		activeMap.put(StatusType.remove, null);
		activeMap.put(StatusType.rebalance, new RebalanceTransitionCallback(
				data, topologyid, currentStatus));
		activeMap.put(StatusType.do_rebalance, null);

		rtn.put(StatusType.active, activeMap);

		// current status is inactive
		Map<StatusType, Callback> inactiveMap = new HashMap<StatusType, Callback>();

		inactiveMap.put(StatusType.monitor, new ReassignTransitionCallback(
				data, topologyid, new StormStatus(StatusType.inactive)));
		inactiveMap.put(StatusType.inactivate, null);
		inactiveMap.put(StatusType.startup, null);
		inactiveMap.put(StatusType.activate, new ActiveTransitionCallback());
		inactiveMap.put(StatusType.kill, new KillTransitionCallback(data,
				topologyid));
		inactiveMap.put(StatusType.remove, null);
		inactiveMap.put(StatusType.rebalance, new RebalanceTransitionCallback(
				data, topologyid, currentStatus));
		inactiveMap.put(StatusType.do_rebalance, null);

		rtn.put(StatusType.inactive, inactiveMap);

		// current status is killed
		Map<StatusType, Callback> killedMap = new HashMap<StatusType, Callback>();

		killedMap.put(StatusType.monitor, null);
		killedMap.put(StatusType.inactivate, null);
		killedMap.put(StatusType.startup, new KillTransitionCallback(data,
				topologyid));
		killedMap.put(StatusType.activate, null);
		killedMap.put(StatusType.kill, new KillTransitionCallback(data,
				topologyid));
		killedMap.put(StatusType.remove, new RemoveTransitionCallback(data,
				topologyid));
		killedMap.put(StatusType.rebalance, null);
		killedMap.put(StatusType.do_rebalance, null);
		rtn.put(StatusType.killed, killedMap);

		// current status is under rebalancing
		Map<StatusType, Callback> rebalancingMap = new HashMap<StatusType, Callback>();

		StatusType rebalanceOldStatus = StatusType.active;
		if (currentStatus.getOldStatus() != null) {
			rebalanceOldStatus = currentStatus.getOldStatus().getStatusType();
			// fix double rebalance, make the status always as rebalacing
			if (rebalanceOldStatus == StatusType.rebalance) {
				rebalanceOldStatus = StatusType.active;
			}
		}

		rebalancingMap.put(StatusType.monitor, null);
		rebalancingMap.put(StatusType.inactivate, null);
		rebalancingMap.put(StatusType.startup, new RebalanceTransitionCallback(
				data, topologyid, new StormStatus(rebalanceOldStatus)));
		rebalancingMap.put(StatusType.activate, null);
		rebalancingMap.put(StatusType.kill, null);
		rebalancingMap.put(StatusType.remove, null);
		rebalancingMap
				.put(StatusType.rebalance, new RebalanceTransitionCallback(
						data, topologyid, currentStatus));
		rebalancingMap.put(StatusType.do_rebalance,
				new DoRebalanceTransitionCallback(data, topologyid,
						new StormStatus(rebalanceOldStatus)));
		rtn.put(StatusType.rebalancing, rebalancingMap);

		/**
		 * @@@ just handling 4 kind of status, maybe add later
		 */

		return rtn;

	}

}
