package com.alibaba.jstorm.message.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.collect.Maps;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

public class NettyUnitTest {

	private static final Logger LOG = Logger.getLogger(NettyUnitTest.class);

	private static int port = 6700;
	private static int task = 1;

	private static String context_class_name = "com.alibaba.jstorm.message.netty.NettyContext";

	private static Map storm_conf = new HashMap<Object, Object>();
	static {
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, context_class_name);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE,
				5 * 1024 * 1024);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		storm_conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
		storm_conf.put(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY,
				"com.lmax.disruptor.BlockingWaitStrategy");
		storm_conf.put(Config.STORM_CLUSTER_MODE, "distributed");
	}

	@Test
	public void test_small_message() {
		System.out.println("!!!!!!!!Start test_small_message !!!!!!!!!!!");
		String req_msg = "Aloha is the most Hawaiian word.";

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		System.out.println("!!!!!!!!!!!!!!!!!!Test one time!!!!!!!!!!!!!!!!!");

		server.close();
		client.close();
		context.term();

		System.out.println("!!!!!!!!!!!!End test_small_message!!!!!!!!!!!!!");
	}
	
	

	public String setupLargMsg() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < Short.MAX_VALUE * 64; i++) {
			sb.append("Aloha is the most Hawaiian word.");
		}

		return sb.toString();
	}

	@Test
	public void test_large_msg() {
		System.out.println("!!!!!!!!!!start larget message test!!!!!!!!");
		String req_msg = setupLargMsg();
		System.out.println("!!!!Finish batch data, size:" + req_msg.length()
				+ "!!!!");

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		client.close();
		server.close();
		context.term();
		System.out.println("!!!!!!!!!!End larget message test!!!!!!!!");
	}

	@Test
	public void test_server_delay() throws InterruptedException {
		System.out.println("!!!!!!!!!!Start delay message test!!!!!!!!");
		String req_msg = setupLargMsg();

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);
		Thread.sleep(1000);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		server.close();
		client.close();
		context.term();
		System.out.println("!!!!!!!!!!End delay message test!!!!!!!!");
	}
	
	@Test
	public void test_first_client() {
		System.out.println("!!!!!!!!Start test_first_client !!!!!!!!!!!");
		String req_msg = setupLargMsg();

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);
		
		JStormUtils.sleepMs(1000);
		System.out.println("!!Client has sent data, but server no start!!!!!");
		
		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		server.close();
		client.close();
		context.term();

		System.out.println("!!!!!!!!!!!!End test_first_client!!!!!!!!!!!!!");
	}

	@Test
	public void test_batch() {
		System.out.println("!!!!!!!!!!Start batch message test!!!!!!!!");

		IContext context = TransportFactory.makeContext(storm_conf);
		final IConnection server = context.bind(null, port);
		IConnection client = null;

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		final int base = 100000;

		List<TaskMessage> list = new ArrayList<TaskMessage>();

		for (int i = 1; i < Short.MAX_VALUE; i++) {

			String req_msg = String.valueOf(i + base);

			TaskMessage message = new TaskMessage(task, req_msg.getBytes());
			list.add(message);

		}

		client.send(list);

		System.out.println("Finish Send ");

		for (int i = 1; i < Short.MAX_VALUE; i++) {
			TaskMessage message = server.recv(0);

			Assert.assertEquals(String.valueOf(i + base),
					new String(message.message()));

			if (i % 1000 == 0) {
				System.out.println("Receive " + i);
			}
		}

		System.out.println("Finish Receive ");

		client.close();
		server.close();
		context.term();
		System.out.println("!!!!!!!!!!End batch message test!!!!!!!!");
	}

	@Test
	public void test_client_reboot() throws InterruptedException {
		System.out.println("!!!!!!!!!!Start client reboot test!!!!!!!!");
		String req_msg = setupLargMsg();

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		client.close();
		Thread.sleep(1000);
		IConnection client2 = context.connect(null, "localhost", port);
		System.out.println("!!!!!!! restart client !!!!!!!!!!");

		client2.send(message);
		Thread.sleep(1000);

		TaskMessage recv2 = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv2.message()));

		client2.close();
		server.close();
		context.term();
		System.out.println("!!!!!!!!!!End client reboot test!!!!!!!!");
	}

	@Test
	public void test_server_reboot() throws InterruptedException {
		System.out.println("!!!!!!!!!!Start server reboot test!!!!!!!!");
		String req_msg = setupLargMsg();

		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;

		server = context.bind(null, port);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), waitStrategy);
		server.registerQueue(recvQueue);

		client = context.connect(null, "localhost", port);

		List<TaskMessage> list = new ArrayList<TaskMessage>();
		TaskMessage message = new TaskMessage(task, req_msg.getBytes());
		list.add(message);

		client.send(message);

		TaskMessage recv = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv.message()));

		server.close();

		client.send(message);
		System.out.println("!!shutdow server and sleep 30s, please wait!!");
		Thread.sleep(30000);

		IConnection server2 = context.bind(null, port);
		server2.registerQueue(recvQueue);
		System.out.println("!!!!!!!!!!!!!!!!!!!! restart server !!!!!!!!!!!");

		TaskMessage recv2 = server2.recv(0);
		Assert.assertEquals(req_msg, new String(recv2.message()));

		client.close();
		server2.close();
		context.term();
		System.out.println("!!!!!!!!!!End server reboot test!!!!!!!!");
	}

}
