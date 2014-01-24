package com.alibaba.jstorm.message;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
//import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

//import org.zeromq.ZMQ.Socket;

/**
 * 
 * @author longda
 * 
 */
public class QueueConnection implements IConnection {
	private static final Logger LOG = LoggerFactory
			.getLogger(QueueConnection.class);
	private final int queue_size;
	private final WaitStrategy waitStrategy;
	private final DisruptorQueue disruptorQueue;
	private boolean closed = false;

	public QueueConnection(Map storm_conf) {
		queue_size = Utils.getInt(storm_conf
				.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
		waitStrategy = (WaitStrategy) Utils.newInstance((String) storm_conf
				.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		disruptorQueue = new DisruptorQueue(new SingleThreadedClaimStrategy(
				queue_size), waitStrategy);
	}

	@Override
	public byte[] recv(int flags) {
		Object obj = null;
		try {
			if ((flags & 0x01) == 0x01) {
				// non-blocking
				obj = disruptorQueue.poll();
			} else {

				obj = disruptorQueue.take();
			}
		} catch (Exception e) {
			LOG.warn("Occur disruptor excpetion ", e);
			return null;
		}

		if (obj != null) {
			return (byte[]) obj;
		} else {
			return null;
		}
	}

	@Override
	public void send(int taskId, byte[] message) {
		disruptorQueue.publish(message);
	}

	@Override
	public void close() {
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

}
