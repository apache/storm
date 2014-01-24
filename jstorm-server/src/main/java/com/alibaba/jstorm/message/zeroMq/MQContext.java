package com.alibaba.jstorm.message.zeroMq;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.message.QueueConnection;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * zeroMQ context
 * 
 * @author yannian/Longda
 * 
 */
public class MQContext implements IContext {
	private final static Logger LOG = LoggerFactory.getLogger(MQContext.class);
	private Map storm_conf;
	private org.zeromq.ZMQ.Context context;
	private int linger_ms;
	private boolean ipc;
	private boolean virtportZmq = false;
	private int maxQueueMsg;
	private Map<Integer, IConnection> queueConnections = new HashMap<Integer, IConnection>();

	@Override
	public void prepare(Map storm_conf) {
		this.storm_conf = storm_conf;
		int zmqThreads = JStormUtils.parseInt(storm_conf
				.get(Config.ZMQ_THREADS));
		linger_ms = JStormUtils.parseInt(storm_conf
				.get(Config.ZMQ_LINGER_MILLIS));
		ipc = StormConfig.cluster_mode(storm_conf).equals("local");
		virtportZmq = JStormUtils.parseBoolean(
				storm_conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
		maxQueueMsg = JStormUtils.parseInt(storm_conf.get(Config.ZMQ_HWM),
				ConfigExtension.DEFAULT_ZMQ_MAX_QUEUE_MSG);
		context = ZeroMq.context(zmqThreads);

		LOG.info("MQContext prepare done...");
	}

	// public IContext makeContext(Map storm_conf) {
	// int zmqThreads =
	// JStormUtils.parseInt(storm_conf.get(Config.ZMQ_THREADS));
	//
	// int linger =
	// JStormUtils.parseInt(storm_conf.get(Config.ZMQ_LINGER_MILLIS));
	//
	// int maxQueueMsg = JStormUtils.parseInt(
	// storm_conf.get(Config.ZMQ_HWM),
	// ConfigExtension.DEFAULT_ZMQ_MAX_QUEUE_MSG);
	//
	// boolean isLocal = StormConfig.cluster_mode(storm_conf).equals("local");
	//
	// // virtport ZMQ will define whether use ZMQ in worker internal
	// commnication
	// boolean virtportZmq = JStormUtils.parseBoolean(
	// storm_conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
	//
	// Context context = ZeroMq.context(zmqThreads);
	//
	// return new MQContext(context, linger, isLocal, virtportZmq, maxQueueMsg);
	//
	// }

	// public static MQContext mk_zmq_context(int num_threads, int linger,
	// boolean local, boolean virtportZmq, int maxQueueMsg) {
	// Context context = ZeroMq.context(num_threads);
	// return new MQContext(context, linger, local, virtportZmq, maxQueueMsg);
	// }

	@SuppressWarnings("unused")
	private MQContext() {
	}

	@Override
	public IConnection bind(String topology_id, int port, boolean distribute) {
		if (distribute || virtportZmq) {
			return zmq_bind(distribute, port);
		} else {
			return queue_bind(port);
		}
	}

	private IConnection zmq_bind(boolean distributeZmq, int port) {
		String url = null;
		if (distributeZmq) {
			if (ipc) {
				url = "ipc://" + port + ".ipc";
			} else {
				url = "tcp://*:" + port;
			}
		} else {
			// virtportZmq will be true
			url = "inproc://" + port;
		}
		Socket socket = ZeroMq.socket(context, ZeroMq.pull);

		ZeroMq.bind(socket, url);
		ZeroMq.set_hwm(socket, maxQueueMsg);

		// ZeroMq.subscribe(socket);

		LOG.info("Create zmq receiver {}", url);
		return new ZMQRecvConnection(socket);
	}

	private IConnection queue_bind(int port) {
		return queue_connect(port);
	}

	@Override
	public IConnection connect(String topology_id, String host, int port,
			boolean distribute) {
		if (distribute || virtportZmq) {
			return zmq_connect(distribute, host, port);
		} else {
			return queue_connect(port);
		}
	}

	private IConnection zmq_connect(boolean distributeZmq, String host, int port) {
		String url = null;

		if (distributeZmq) {
			if (ipc) {
				url = "ipc://" + port + ".ipc";
			} else {
				url = "tcp://" + host + ":" + port;
			}
		} else {
			// virtportZmq will be true
			url = "inproc://" + port;
		}

		Socket socket = ZeroMq.socket(context, ZeroMq.push);
		socket = ZeroMq.set_linger(socket, linger_ms);
		socket = ZeroMq.connect(socket, url);
		ZeroMq.set_hwm(socket, maxQueueMsg);

		LOG.info("Create zmq sender {}", url);
		return new ZMQSendConnection(socket);
	}

	private IConnection queue_connect(int port) {
		IConnection queueConnection = null;
		synchronized (this) {
			queueConnection = queueConnections.get(port);
			if (queueConnection == null) {
				queueConnection = new QueueConnection(storm_conf);

				queueConnections.put(port, queueConnection);

				LOG.info("Create internal queue connect {}", port);
			}
		}

		return queueConnection;
	}

	public void term() {
		LOG.info("ZMQ context terminates ");
		context.term();
	}

	public Context getContext() {
		return context;
	}

}
