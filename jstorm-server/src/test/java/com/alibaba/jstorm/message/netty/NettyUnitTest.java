package com.alibaba.jstorm.message.netty;

import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.google.common.collect.Maps;

public class NettyUnitTest {
    
    private static final Logger LOG = Logger.getLogger(NettyUnitTest.class);
	
	private static int port = 6700;
	private static int task = 1;
	
	private static String context_class_name = "com.alibaba.jstorm.message.netty.NettyContext";
	
	
	@Test
	public void test_basic() {
	    LOG.info("!!!!!!!!!!!!!!!!!!!!Start basic test!!!!!!!!!!!!!!!!!!!");
		String req_msg = "Aloha is the most Hawaiian word.";
		Map storm_conf = Maps.newHashMap();
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, context_class_name);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		storm_conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
	    storm_conf.put(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY, "com.lmax.disruptor.BlockingWaitStrategy");
	    
		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;
		server = context.bind(null, port, true);
		client = context.connect(null, "localhost", port, true);
		client.send(task, req_msg.getBytes());
		byte[] recv_msg = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv_msg));
		
		LOG.info("!!!!!!!!!!!!!!!!!!!!Test one time!!!!!!!!!!!!!!!!!!!");
		
		
		server.close();
		client.close();
		context.term();
		
		LOG.info("!!!!!!!!!!!!!!!!!!!!End basic test!!!!!!!!!!!!!!!!!!!");
	}
	
	@Test
	public void test_large_msg() {
		String req_msg = "";
		for (int i = 0; i < Short.MAX_VALUE; i++) {
			req_msg += "Aloha";
		}
		Map storm_conf = Maps.newHashMap();
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, context_class_name);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 102400);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		storm_conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
	    storm_conf.put(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY, "com.lmax.disruptor.BlockingWaitStrategy");
		
		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;
		server = context.bind(null, port, true);
		client = context.connect(null, "localhost", port, true);
		client.send(task, req_msg.getBytes());
		byte[] recv_msg = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv_msg));
		
		server.close();
		client.close();
		context.term();
	}
	
	@Test
	public void test_server_delay() throws InterruptedException {
		String req_msg = "Aloha is the most Hawaiian word.";
		Map storm_conf = Maps.newHashMap();
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, context_class_name);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		storm_conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
	    storm_conf.put(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY, "com.lmax.disruptor.BlockingWaitStrategy");
		
		IContext context = TransportFactory.makeContext(storm_conf);
		IConnection server = null;
		IConnection client = null;
		client = context.connect(null, "localhost", port, true);
		client.send(task, req_msg.getBytes());
		Thread.sleep(1000);
		server = context.bind(null, port, true);
		byte[] recv_msg = server.recv(0);
		Assert.assertEquals(req_msg, new String(recv_msg));
		
		server.close();
		client.close();
		context.term();
	}
	
	@Test
	public void test_batch() {
		Map storm_conf = Maps.newHashMap();
		storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, context_class_name);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
		storm_conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
	    storm_conf.put(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY, "com.lmax.disruptor.BlockingWaitStrategy");
		
		IContext context = TransportFactory.makeContext(storm_conf);
		final IConnection server = context.bind(null, port, true);
		IConnection client = null;
		client = context.connect(null, "localhost", port, true);
		
		final int base = 100000;
		
		Thread consumer = new Thread(new Runnable(){
		    
			@Override
			public void run() {
				for (int i = 1; i < 10000; i++) {
					byte[] recv_msg = server.recv(0);
					Assert.assertEquals(String.valueOf(i + base), new String(recv_msg));
					
					LOG.info("Receive " + i);
				}
				
				LOG.info("Finish Receive " );
			}
			
		}, "Btach-Consumer-Thread");
		consumer.start();
		for (int i = 1; i < 10000; i++) {
		    
			client.send(task, String.valueOf(base + i).getBytes());
			if (i % 100 == 0) {
                LOG.info("Send " + i);
            }
		}
		LOG.info("Finish Send ");
		
		try {
			consumer.join();
		} catch (InterruptedException e) {
			throw new RuntimeException (e);
		}

		client.close();
		server.close();
		context.term();
	}
	
}
