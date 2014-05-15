package com.alibaba.jstorm.drpc;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.server.THsHaServer;
import org.apache.thrift7.transport.TNonblockingServerSocket;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.DistributedRPCInvocations;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * Drpc
 * 
 * @author yannian
 * 
 */
public class Drpc implements DistributedRPC.Iface,
		DistributedRPCInvocations.Iface, Shutdownable {

	private static final Logger LOG = Logger.getLogger(Drpc.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Begin to start Drpc server");

		final Drpc service = new Drpc();

		service.init();
	}

	private Map conf;

	private THsHaServer handlerServer;

	private THsHaServer invokeServer;

	private AsyncLoopThread clearThread;

	private AtomicBoolean isActive = new AtomicBoolean(true);

	private THsHaServer initHandlerServer(Map conf, final Drpc service)
			throws Exception {
		int port = JStormUtils.parseInt(conf.get(Config.DRPC_PORT));
		int workerThreadNum = JStormUtils.parseInt(conf.get(Config.DRPC_WORKER_THREADS));
		int queueSize = JStormUtils.parseInt(conf.get(Config.DRPC_QUEUE_SIZE));

		TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
		THsHaServer.Args targs = new THsHaServer.Args(socket);
		targs.workerThreads(64);
		targs.protocolFactory(new TBinaryProtocol.Factory());
		targs.processor(new DistributedRPC.Processor<DistributedRPC.Iface>(
				service));
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(workerThreadNum, 
				workerThreadNum, 60, TimeUnit.SECONDS, 
				new ArrayBlockingQueue(queueSize));
		targs.executorService(executor);

		THsHaServer handlerServer = new THsHaServer(targs);
		LOG.info("Successfully init Handler Server " + port);

		return handlerServer;
	}

	private THsHaServer initInvokeServer(Map conf, final Drpc service)
			throws Exception {
		int port = JStormUtils.parseInt(conf.get(Config.DRPC_INVOCATIONS_PORT));

		TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
		THsHaServer.Args targsInvoke = new THsHaServer.Args(socket);
		targsInvoke.workerThreads(64);
		targsInvoke.protocolFactory(new TBinaryProtocol.Factory());
		targsInvoke
				.processor(new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(
						service));

		THsHaServer invokeServer = new THsHaServer(targsInvoke);

		LOG.info("Successfully init Invoke Server " + port);
		return invokeServer;
	}

	private void initThrift() throws Exception {

		handlerServer = initHandlerServer(conf, this);

		invokeServer = initInvokeServer(conf, this);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				Drpc.this.shutdown();
				handlerServer.stop();
				invokeServer.stop();
			}

		});

		LOG.info("Starting Distributed RPC servers...");
		new Thread(new Runnable() {
			@Override
			public void run() {
				invokeServer.serve();
			}
		}).start();
		handlerServer.serve();
	}

	private void initClearThread() {
		clearThread = new AsyncLoopThread(new ClearThread(this));
		LOG.info("Successfully start clear thread");
	}

	public void init() throws Exception {
		conf = StormConfig.read_storm_config();
		LOG.info("Configuration is \n" + conf);
		
		initClearThread();

		initThrift();
	}

	public Drpc() {

	}
	
	@Override
	public void shutdown() {
		isActive.set(false);
		
		clearThread.interrupt();
		
		try {
			clearThread.join();
		} catch (InterruptedException e) {
		}
		LOG.info("Successfully cleanup clear thread");
		
		invokeServer.stop();
		LOG.info("Successfully stop invokeServer");
		
		handlerServer.stop();
		LOG.info("Successfully stop handlerServer");
		
	}
	

	private AtomicInteger ctr = new AtomicInteger(0);
	private ConcurrentHashMap<String, Semaphore> idtoSem = new ConcurrentHashMap<String, Semaphore>();
	private ConcurrentHashMap<String, Object> idtoResult = new ConcurrentHashMap<String, Object>();
	private ConcurrentHashMap<String, Integer> idtoStart = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();

	public void cleanup(String id) {
		LOG.info("clean id " + id + " @ " + (System.currentTimeMillis()));

		idtoSem.remove(id);
		idtoResult.remove(id);
		idtoStart.remove(id);
	}

	@Override
	public String execute(String function, String args)
			throws DRPCExecutionException, TException {
		LOG.info("Received DRPC request for " + function + " " + args + " at "
				+ (System.currentTimeMillis()));
		int idinc = this.ctr.incrementAndGet();
		int maxvalue = 1000000000;
		int newid = idinc % maxvalue;
		if (idinc != newid) {
			this.ctr.compareAndSet(idinc, newid);
		}

		String strid = String.valueOf(newid);
		Semaphore sem = new Semaphore(0);

		DRPCRequest req = new DRPCRequest(args, strid);
		this.idtoStart.put(strid, TimeUtils.current_time_secs());
		this.idtoSem.put(strid, sem);
		ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(function);
		queue.add(req);
		LOG.info("Waiting for DRPC request for " + function + " " + args
				+ " at " + (System.currentTimeMillis()));
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			LOG.error("acquire fail ", e);
		}
		LOG.info("Acquired for DRPC request for " + function + " " + args
				+ " at " + (System.currentTimeMillis()));

		Object result = this.idtoResult.get(strid);
		LOG.info("Returning for DRPC request for " + function + " " + args
				+ " at " + (System.currentTimeMillis()));

		this.cleanup(strid);

		if (result instanceof DRPCExecutionException) {
			throw (DRPCExecutionException) result;
		}
		return String.valueOf(result);
	}

	@Override
	public void result(String id, String result) throws TException {
		Semaphore sem = this.idtoSem.get(id);
		LOG.info("Received result " + result + " for id " + id + " at "
				+ (System.currentTimeMillis()));
		if (sem != null) {
			this.idtoResult.put(id, result);
			sem.release();
		}

	}

	@Override
	public DRPCRequest fetchRequest(String functionName) throws TException {

		ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
		DRPCRequest req = queue.poll();
		if (req != null) {
			LOG.info("Fetched request for " + functionName + " at "
					+ (System.currentTimeMillis()));
			return req;
		}else {
			return new DRPCRequest("", "");
		}
		
		
	}

	@Override
	public void failRequest(String id) throws TException {
		Semaphore sem = this.idtoSem.get(id);
		LOG.info("failRequest result  for id " + id + " at "
				+ (System.currentTimeMillis()));
		if (sem != null) {
			this.idtoResult.put(id,
					new DRPCExecutionException("Request failed"));
			sem.release();
		}
	}

	

	private ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
		ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
		if (reqQueue == null) {
			reqQueue = new ConcurrentLinkedQueue<DRPCRequest>();
			requestQueues.put(function, reqQueue);
		}
		return reqQueue;
	}

	public ConcurrentHashMap<String, Semaphore> getIdtoSem() {
		return idtoSem;
	}

	public ConcurrentHashMap<String, Object> getIdtoResult() {
		return idtoResult;
	}

	public ConcurrentHashMap<String, Integer> getIdtoStart() {
		return idtoStart;
	}

	public AtomicBoolean getIsActive() {
		return isActive;
	}

	public Map getConf() {
		return conf;
	}

	
}
