package com.alibaba.jstorm.task.acker;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;

/**
 * 
 * @author yannian/Longda
 * 
 */
public class Acker implements IBolt {

	private static final Logger LOG = LoggerFactory.getLogger(Acker.class);

	private static final long serialVersionUID = 4430906880683183091L;

	public static final String ACKER_COMPONENT_ID = "__acker";
	public static final String ACKER_INIT_STREAM_ID = "__ack_init";
	public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
	public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";

	public static final int TIMEOUT_BUCKET_NUM = 3;

	private OutputCollector collector = null;
	// private TimeCacheMap<Object, AckObject> pending = null;
	private RotatingMap<Object, AckObject> pending = null;
	private long lastRotate = System.currentTimeMillis();
	private long rotateTime;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		// pending = new TimeCacheMap<Object, AckObject>(timeoutSec,
		// TIMEOUT_BUCKET_NUM);
		this.pending = new RotatingMap<Object, AckObject>(TIMEOUT_BUCKET_NUM);
		this.rotateTime = 1000L * JStormUtils.parseInt(
				stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30)/
				(TIMEOUT_BUCKET_NUM - 1);
	}

	@Override
	public void execute(Tuple input) {
		Object id = input.getValue(0);

		AckObject curr = pending.get(id);

		String stream_id = input.getSourceStreamId();

		if (Acker.ACKER_INIT_STREAM_ID.equals(stream_id)) {
			if (curr == null) {
				curr = new AckObject();

				curr.val = input.getLong(1);
				curr.spout_task = input.getInteger(2);

				pending.put(id, curr);
			} else {
				// bolt's ack first come
				curr.update_ack(input.getValue(1));
				curr.spout_task = input.getInteger(2);
			}

		} else if (Acker.ACKER_ACK_STREAM_ID.equals(stream_id)) {
			if (curr != null) {
				curr.update_ack(input.getValue(1));

			} else {
				// two case
				// one is timeout
				// the other is bolt's ack first come
				curr = new AckObject();

				curr.val = Long.valueOf(input.getLong(1));

				pending.put(id, curr);

			}
		} else if (Acker.ACKER_FAIL_STREAM_ID.equals(stream_id)) {
			if (curr == null) {
				// do nothing
				// already timeout, should go fail
				return;
			}

			curr.failed = true;

		} else {
			LOG.info("Unknow source stream");
			return;
		}

		Integer task = curr.spout_task;

		if (task != null) {

			if (curr.val == 0) {
				pending.remove(id);
				List values = JStormUtils.mk_list(id);

				collector.emitDirect(task, Acker.ACKER_ACK_STREAM_ID, values);

			} else {

				if (curr.failed) {
					pending.remove(id);
					List values = JStormUtils.mk_list(id);
					collector.emitDirect(task, Acker.ACKER_FAIL_STREAM_ID,
							values);
				}
			}
		} else {

		}

		// add this operation to update acker's ACK statics
		collector.ack(input);

		long now = System.currentTimeMillis();
		if (now - lastRotate > rotateTime) {
			lastRotate = now;
			Map<Object, AckObject> tmp = pending.rotate();
			LOG.info("Acker's timeout item size:{}", tmp.size());
		}

	}

	@Override
	public void cleanup() {
		LOG.info("Successfully cleanup");
	}

}
