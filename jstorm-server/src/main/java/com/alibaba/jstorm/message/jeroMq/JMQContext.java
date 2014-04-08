package com.alibaba.jstorm.message.jeroMq;

import java.util.Map;

import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;

import com.alibaba.jstorm.message.zeroMq.MQContext;

public class JMQContext extends MQContext {
	
	private Context context;

	@Override
	public void prepare(Map storm_conf) {
		super.prepare(storm_conf);
	}
	
	protected void init() {
		context = ZMQ.context(zmqThreads);
	}
	
	@SuppressWarnings("unused")
	private JMQContext() {
		super();
	}

	@Override
	public void term() {
		LOG.info("JZMQ context terminates ");
		context.term();
	}
	
	public IConnection zmq_bind(boolean distributeZmq, int port) {
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
		
		Socket socket = context.socket(ZMQ.PULL);
		socket.connect(url);
		socket.setHWM(maxQueueMsg);

		LOG.info("Create jzmq receiver {}", url);
		return new JZMQRecvConnection(socket);
	}
	
	public IConnection zmq_connect(boolean distributeZmq, String host, int port) {
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

		Socket socket = context.socket (ZMQ.PUSH);
		socket.bind(url);
		socket.setLinger(linger_ms);
		socket.setHWM(maxQueueMsg);

		LOG.info("Create jzmq sender {}", url);
		return new JZMQSendConnection(socket);
	}

}
