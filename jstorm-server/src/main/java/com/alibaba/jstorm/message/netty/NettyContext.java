package com.alibaba.jstorm.message.netty;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.message.QueueConnection;

public class NettyContext implements IContext {
	private final static Logger LOG = LoggerFactory
			.getLogger(NettyContext.class);
	@SuppressWarnings("rawtypes")
	private Map storm_conf;
	// private volatile Vector<IConnection> connections;
	private Map<Integer, IConnection> queueConnections;

	@SuppressWarnings("unused")
	public NettyContext() {
	}

	/**
	 * initialization per Storm configuration
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map storm_conf) {
		this.storm_conf = storm_conf;
		// connections = new Vector<IConnection>();
		queueConnections = new HashMap<Integer, IConnection>();
	}

	@Override
	public IConnection bind(String topology_id, int port, boolean distribute) {
		if (distribute && !StormConfig.local_mode(storm_conf)) {
			return netty_bind(topology_id, port);
		} else {
			return queue_bind(port);
		}
	}

	private IConnection netty_bind(String topology_id, int port) {
		// IConnection server = new NettyServer(storm_conf, port);
		// connections.add(server);
		// return server;
		return new NettyServer(storm_conf, port);
	}

	private IConnection queue_bind(int port) {
		return queue_connect(port);
	}

	@Override
	public IConnection connect(String topology_id, String host, int port,
			boolean distribute) {
		if (distribute && !StormConfig.local_mode(storm_conf)) {
			return netty_connect(topology_id, host, port);
		} else {
			return queue_connect(port);
		}
	}

	private IConnection netty_connect(String topology_id, String host, int port) {
		// IConnection client = new NettyClient(storm_conf, host, port);
		// connections.add(client);
		// return client;
		return new NettyClient(storm_conf, host, port);
	}

	private IConnection queue_connect(int port) {
		IConnection queueConnection = null;
		synchronized (this) {
			queueConnection = queueConnections.get(port);
			if (queueConnection == null) {
				queueConnection = new QueueConnection(storm_conf);

				queueConnections.put(port, queueConnection);

				LOG.info("Create internal queue connect {}", port);
			} else {
				LOG.info("Reuse internal queue connect {}", port);
			}
		}

		return queueConnection;
	}

	/**
	 * terminate this context
	 */
	public void term() {
		// for (IConnection conn : connections) {
		// conn.close();
		// }
		// connections = null;
	}

}
