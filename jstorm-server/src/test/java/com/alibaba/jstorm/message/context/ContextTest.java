package com.alibaba.jstorm.message.context;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;

public class ContextTest {

	@Test
	public void test_zmq() throws Exception {
		String klassName = "com.alibaba.jstorm.message.zeroMq.MQContext";
		Class klass = Class.forName(klassName);
		Constructor<IContext> constructor = klass.getDeclaredConstructor();
		constructor.setAccessible(true);
		IContext context = (IContext) constructor.newInstance();
		Assert.assertNotNull(context);
	}

	@Test
	public void test_netty() {
		Map storm_conf = Maps.newHashMap();
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT,
				"com.alibaba.jstorm.message.netty.NettyContext");
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		IContext context = TransportFactory.makeContext(storm_conf);
		Assert.assertNotNull(context);
	}
}
