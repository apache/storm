package com.alibaba.jstorm.message.zeroMq;

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
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * zeroMQ context
 * 
 * @author yannian/Longda/zhiyuan.ls
 * 
 */
public class MQContext implements IContext {
	protected final static Logger LOG = LoggerFactory.getLogger(MQContext.class);
	protected Map storm_conf;
	protected int zmqThreads;
	protected int linger_ms;
	protected boolean ipc;
	protected boolean virtportZmq = false;
	protected int maxQueueMsg;
	
	private Context context;
	
	@Override
	public void prepare(Map storm_conf) {
		this.storm_conf = storm_conf;
		zmqThreads = JStormUtils.parseInt(storm_conf
				.get(Config.ZMQ_THREADS));
		linger_ms = JStormUtils.parseInt(storm_conf
				.get(Config.ZMQ_LINGER_MILLIS));
		ipc = StormConfig.cluster_mode(storm_conf).equals("local");
		virtportZmq = JStormUtils.parseBoolean(
				storm_conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
		maxQueueMsg = JStormUtils.parseInt(storm_conf.get(Config.ZMQ_HWM),
				ConfigExtension.DEFAULT_ZMQ_MAX_QUEUE_MSG);
		init();
		
		LOG.info("MQContext prepare done...");
	}
	
	protected void init() {
		context = ZeroMq.context(zmqThreads);
	}


	@SuppressWarnings("unused")
	protected MQContext() {
	}

	
	@Override
	public IConnection bind(String topology_id, int port) {

		return zmq_bind(true, port);

	}

	protected IConnection zmq_bind(boolean distributeZmq, int port) {
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

	
	@Override
	public IConnection connect(String topology_id, String host, int port) {
		return zmq_connect(true, host, port);
	}

	protected IConnection zmq_connect(boolean distributeZmq, String host, int port) {
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
		return new ZMQSendConnection(socket, host, port);
	}

	public void term() {
		LOG.info("ZMQ context terminates ");
		context.term();
	}


//	public Context getContext() {
//		return context;
//	}

}
