package com.alibaba.jstorm.message.netty;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.RunnableCallback;

public class ReconnectRunnable extends RunnableCallback{
	private static final Logger LOG = Logger.getLogger(ReconnectRunnable.class);
	
	
	private BlockingQueue<NettyClient> queue = new LinkedBlockingDeque<NettyClient>();
	public void pushEvent(NettyClient client) {
		queue.offer(client);
	}
	
	private boolean closed = false;
	private Thread thread = null;
	
	
	
	@Override
	public void run() {
		LOG.info("Successfully start reconnect thread");
		thread = Thread.currentThread();
		while(closed == false) {
			NettyClient client = null;
			try {
				client = queue.take();
			} catch (InterruptedException e) {
				continue;
			}
			if (client != null) {
				client.doReconnect();
			}
			
		}
		
		LOG.info("Successfully shutdown reconnect thread");
	}

	@Override
	public void shutdown() {
		closed = true;
		thread.interrupt();
	}
	
	@Override
	public Object getResult() {
		return -1;
	}
}
