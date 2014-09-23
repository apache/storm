package com.alibaba.jstorm.message.netty;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.Utils;

public class NettyContext implements IContext {
	private final static Logger LOG = LoggerFactory
			.getLogger(NettyContext.class);
	@SuppressWarnings("rawtypes")
	private Map storm_conf;
	
	private NioClientSocketChannelFactory clientChannelFactory;

    private ScheduledExecutorService clientScheduleService;
    private final int MAX_CLIENT_SCHEDULER_THREAD_POOL_SIZE = 10;
    
    private ReconnectRunnable reconnector;
    
    private boolean isSyncMode = false;

	@SuppressWarnings("unused")
	public NettyContext() {
	}

	/**
	 * initialization per Storm configuration
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map storm_conf) {
		this.storm_conf = storm_conf;
		
		int maxWorkers = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
        ThreadFactory bossFactory = new NettyRenameThreadFactory(NettyClient.PREFIX + "boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory(NettyClient.PREFIX + "worker");

		if (maxWorkers > 0) {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory));
        }
        int otherWorkers = Utils.getInt(storm_conf.get(Config.TOPOLOGY_WORKERS), 1) - 1;
        int poolSize = Math.min(Math.max(1, otherWorkers), MAX_CLIENT_SCHEDULER_THREAD_POOL_SIZE);
        clientScheduleService = Executors.newScheduledThreadPool(poolSize, new NettyRenameThreadFactory("client-schedule-service"));
	
        reconnector = new ReconnectRunnable();
        new AsyncLoopThread(reconnector, true, Thread.MIN_PRIORITY, true);
        
        isSyncMode = ConfigExtension.isNettySyncMode(storm_conf);
	}

	@Override
	public IConnection bind(String topology_id, int port) {
		IConnection retConnection = null;
		try {

			retConnection = new NettyServer(storm_conf, port, isSyncMode);
		} catch (Throwable e) {
			LOG.error("Failed to instance NettyServer", e.getCause());
			JStormUtils.halt_process(-1, "Failed to bind " + port);
		}

		return retConnection;
	}

	@Override
	public IConnection connect(String topology_id, String host, int port) {
		if (isSyncMode == true) {
			return new NettyClientSync(storm_conf, clientChannelFactory, clientScheduleService, host, port, reconnector);
		}else {
			return new NettyClientAsync(storm_conf, clientChannelFactory, clientScheduleService, host, port, reconnector);
		}
	}


	/**
	 * terminate this context
	 */
	public void term() {
        clientScheduleService.shutdown();        
		// for (IConnection conn : connections) {
		// conn.close();
		// }
        try {
            clientScheduleService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }
		// connections = null;
		
		clientChannelFactory.releaseExternalResources();
		
		reconnector.shutdown();
	}

}
