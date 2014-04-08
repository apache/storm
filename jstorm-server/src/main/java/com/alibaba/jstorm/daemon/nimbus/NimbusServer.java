package com.alibaba.jstorm.daemon.nimbus;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.ThriftResourceType;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.schedule.CleanRunnable;
import com.alibaba.jstorm.schedule.FollowerRunnable;
import com.alibaba.jstorm.schedule.MonitorRunnable;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * 
 * NimbusServer work flow: 1. cleanup interrupted topology delete
 * /storm-local-dir/nimbus/topologyid/stormdis delete
 * /storm-zk-root/storms/topologyid
 * 
 * 2. set /storm-zk-root/storms/topology stats as run
 * 
 * 3. start one thread, every nimbus.monitor.reeq.secs set
 * /storm-zk-root/storms/ all topology as monitor. when the topology's status is
 * monitor, nimubs would reassign workers 4. start one threa, every
 * nimubs.cleanup.inbox.freq.secs cleanup useless jar
 * 
 * @author version 1: Nathan Marz version 2: Lixin/Chenjun version 3: Longda
 * 
 */
public class NimbusServer {

	private static final Logger LOG = Logger.getLogger(NimbusServer.class);

	private NimbusData data;

	private ServiceHandler serviceHandler;

	private TopologyAssign topologyAssign;

	private THsHaServer thriftServer;

	private FollowerRunnable follower;
	
	private AtomicBoolean    isShutdown = new AtomicBoolean(false);

	public static void main(String[] args) throws Exception {
		// read configuration files
		@SuppressWarnings("rawtypes")
		Map config = Utils.readStormConfig();
		NimbusServer instance = new NimbusServer();

		INimbus iNimbus = new DefaultInimbus();

		instance.launchServer(config, iNimbus);

	}

	@SuppressWarnings("rawtypes")
	private void launchServer(final Map conf, INimbus inimbus)  {
		LOG.info("Begin to start nimbus with conf " + conf);

		try {
			// 1. check whether mode is distributed or not
			StormConfig.validate_distributed_mode(conf);
	
			initShutdownHook();
	
			inimbus.prepare(conf, StormConfig.masterInimbus(conf));
	
			data = createNimbusData(conf, inimbus);
	
			initFollowerThread(conf);
			
			while (!data.isLeader())
				Utils.sleep(5000);
			
			init(conf);
		}catch (Throwable e) {
			LOG.error("Fail to run nimbus ", e);
		}finally {
			cleanup();
		}
		
		LOG.info("Quit nimbus");
	}

	public ServiceHandler launcherLocalServer(final Map conf, INimbus inimbus)
			throws Exception {
		LOG.info("Begin to start nimbus on local model");

		StormConfig.validate_local_mode(conf);

		inimbus.prepare(conf, StormConfig.masterInimbus(conf));

		data = createNimbusData(conf, inimbus);

		init(conf);

		return serviceHandler;
	}

	private void init(Map conf) throws Exception {

		NimbusUtils.cleanupCorruptTopologies(data);

		initTopologyAssign();

		initTopologyStatus();

		initMonitor(conf);

		initCleaner(conf);

		serviceHandler = new ServiceHandler(data);

		if (!data.isLocalMode()) {

			initGroup(conf);

			initThrift(conf);
		}
	}

	@SuppressWarnings("rawtypes")
	private NimbusData createNimbusData(Map conf, INimbus inimbus)
			throws Exception {

		TimeCacheMap.ExpiredCallback<Object, Object> expiredCallback = new TimeCacheMap.ExpiredCallback<Object, Object>() {
			@Override
			public void expire(Object key, Object val) {
				try {
					LOG.info("Close file " + String.valueOf(key));
					if (val != null) {
						if (val instanceof Channel) {
							Channel channel = (Channel) val;
							channel.close();
						} else if (val instanceof BufferFileInputStream) {
							BufferFileInputStream is = (BufferFileInputStream) val;
							is.close();
						}
					}
				} catch (IOException e) {
					LOG.error(e.getMessage(), e);
				}

			}
		};

		int file_copy_expiration_secs = JStormUtils.parseInt(
				conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
		TimeCacheMap<Object, Object> uploaders = new TimeCacheMap<Object, Object>(
				file_copy_expiration_secs, expiredCallback);
		TimeCacheMap<Object, Object> downloaders = new TimeCacheMap<Object, Object>(
				file_copy_expiration_secs, expiredCallback);

		// Callback callback=new TimerCallBack();
		// StormTimer timer=Timer.mkTimerTimer(callback);
		NimbusData data = new NimbusData(conf, downloaders, uploaders, inimbus);

		return data;

	}

	private void initTopologyAssign() {
		topologyAssign = TopologyAssign.getInstance();
		topologyAssign.init(data);
	}

	private void initTopologyStatus() throws Exception {
		// get active topology in ZK
		List<String> active_ids = data.getStormClusterState().active_storms();

		if (active_ids != null) {

			for (String topologyid : active_ids) {
				// set the topology status as startup
				// in fact, startup won't change anything
				NimbusUtils.transition(data, topologyid, false,
						StatusType.startup);
			}

		}

		LOG.info("Successfully init topology status");
	}

	@SuppressWarnings("rawtypes")
	private void initMonitor(Map conf) {
		final ScheduledExecutorService scheduExec = data.getScheduExec();

		// Schedule Nimbus monitor
		MonitorRunnable r1 = new MonitorRunnable(data);

		int monitor_freq_secs = JStormUtils.parseInt(
				conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
		scheduExec.scheduleAtFixedRate(r1, 0, monitor_freq_secs,
				TimeUnit.SECONDS);

		LOG.info("Successfully init Monitor thread");
	}

	/**
	 * Right now, every 600 seconds, nimbus will clean jar under
	 * /LOCAL-DIR/nimbus/inbox, which is the uploading topology directory
	 * 
	 * @param conf
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	private void initCleaner(Map conf) throws IOException {
		final ScheduledExecutorService scheduExec = data.getScheduExec();

		// Schedule Nimbus inbox cleaner/nimbus/inbox jar
		String dir_location = StormConfig.masterInbox(conf);
		int inbox_jar_expiration_secs = JStormUtils.parseInt(
				conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
		CleanRunnable r2 = new CleanRunnable(dir_location,
				inbox_jar_expiration_secs);

		int cleanup_inbox_freq_secs = JStormUtils.parseInt(
				conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);

		scheduExec.scheduleAtFixedRate(r2, 0, cleanup_inbox_freq_secs,
				TimeUnit.SECONDS);

		LOG.info("Successfully init " + dir_location + " cleaner");
	}

	@SuppressWarnings("rawtypes")
	private void initThrift(Map conf) throws TTransportException {
		Integer thrift_port = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT));
		TNonblockingServerSocket socket = new TNonblockingServerSocket(
				thrift_port);
		
		Integer maxReadBufSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE));
		
		THsHaServer.Args args = new THsHaServer.Args(socket);
		args.workerThreads(ServiceHandler.THREAD_NUM);
		args.protocolFactory(new TBinaryProtocol.Factory(false, true, maxReadBufSize));

		args.processor(new Nimbus.Processor<Iface>(serviceHandler));
		args.maxReadBufferBytes = maxReadBufSize;
		
		thriftServer = new THsHaServer(args);

		LOG.info("Successfully started nimbus: started Thrift server...");
		thriftServer.serve();
	}

	private void initFollowerThread(Map conf) {
		follower = new FollowerRunnable(data, 5000);
		Thread thread = new Thread(follower);
		thread.setDaemon(true);
		thread.start();
		LOG.info("Successfully init Follower thread");
	}

	private void flushGroupFile(String filePath, NimbusData data)
			throws Exception {

		data.getFlushGroupFileLock().lock();
		LOG.info("Begin to sync the group file: " + filePath);
		Map<String, Map<ThriftResourceType, Integer>> groupToResource = data
				.getGroupToResource();
		groupToResource.clear();
		try {
			org.ini4j.Config conf = new org.ini4j.Config();
			conf.setMultiSection(true);
			Ini ini = new Ini();
			ini.setConfig(conf);
			File file = new File(filePath);
			ini.load(file);
			List<Section> groupings = ini.getAll("group");
			for (Section section : groupings) {
				String name = section.get("NAME");
				if (name == null)
					continue;
				Map<ThriftResourceType, Integer> resource = groupToResource
						.get(name);
				if (resource == null) {
					resource = new HashMap<ThriftResourceType, Integer>();
					groupToResource.put(name, resource);
				}
				resource.put(ThriftResourceType.CPU, getResourceNum(section
						.get(ThriftResourceType.CPU.name())));
				resource.put(ThriftResourceType.MEM, getResourceNum(section
						.get(ThriftResourceType.MEM.name())));
				resource.put(ThriftResourceType.DISK, getResourceNum(section
						.get(ThriftResourceType.DISK.name())));
				resource.put(ThriftResourceType.NET, getResourceNum(section
						.get(ThriftResourceType.NET.name())));
			}
			cleanGroup(data);
			LOG.info("Successfully sync the group file: " + filePath);
		} catch (Exception e) {
			LOG.error("Flush group file error " + filePath, e);
			throw e;
		} finally {
			data.getFlushGroupFileLock().unlock();
		}
	}

	private void cleanGroup(NimbusData data) {
		Map<String, Map<ThriftResourceType, Integer>> groupToResource = data
				.getGroupToResource();
		Map<String, Map<ThriftResourceType, Integer>> groupToUsedResource = data
				.getGroupToUsedResource();
		for (String group : groupToUsedResource.keySet()) {
			if (groupToResource.get(group) == null) {
				Map<ThriftResourceType, Integer> usedResource = groupToUsedResource
						.get(group);

				boolean delete = true;
				for (Entry<ThriftResourceType, Integer> entry : usedResource
						.entrySet()) {
					if (entry.getValue().intValue() != 0) {
						delete = false;
						break;
					}
				}
				if (delete)
					groupToUsedResource.remove(group);
			}
		}
	}

	private int getResourceNum(String num) {
		int result = (num == null) ? Integer.MAX_VALUE : Integer.valueOf(num);
		return result < 0 ? Integer.MAX_VALUE : result;
	}

	private void initGroupFileWatcher(final String filePath,
			final NimbusData data) {
		try {

			String normalizePath = PathUtils.normalize_path(filePath);

			if (PathUtils.exists_file(normalizePath) == false) {
				LOG.warn("Grouping file doesn't exist " + filePath);
				throw new RuntimeException("Grouping file doesn't exist "
						+ filePath);
			}

			List<String> paths = PathUtils.tokenize_path(normalizePath);
			String fileName = paths.get(paths.size() - 1);
			String path = PathUtils.parent_path(normalizePath);

			FileAlterationObserver observer = new FileAlterationObserver(path,
					null);
			FileListenerAdaptor listener = new FileListenerAdaptor(fileName,
					normalizePath, data);
			observer.addListener(listener);
			final FileAlterationMonitor fileMonitor = new FileAlterationMonitor(
					10000, new FileAlterationObserver[] { observer });
			Runnable runnable = new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					try {
						fileMonitor.start();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						LOG.error("init group_file watcher error!", e);
						throw new RuntimeException();
					}
				}

			};
			Thread thread = new Thread(runnable);
			thread.setDaemon(true);
			thread.start();
			LOG.info("Successfully init gorup_file watcher !" + normalizePath);
		} catch (Exception e) {
			LOG.error("Failed to init group_file watcher " + filePath, e);
			throw new RuntimeException();
		}
	}

	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				NimbusServer.this.cleanup();
			}

		});
	}

	public void cleanup() {
		if (isShutdown.compareAndSet(false, true) == false ) {
			LOG.info("Notify to quit nimbus");
			return ;
		}
		
		LOG.info("Begin to shutdown nimbus");
		
		if (serviceHandler != null) {
			serviceHandler.shutdown();
		}

		if (topologyAssign != null) {
			topologyAssign.cleanup();
			LOG.info("Successfully shutdown TopologyAssign thread");
		}

		if (follower != null) {
			follower.clean();
			LOG.info("Successfully shutdown follower thread");
		}

		if (data != null) {
			data.cleanup();
			LOG.info("Successfully shutdown NimbusData");
		}

		if (thriftServer != null) {
			thriftServer.stop();
			LOG.info("Successfully shutdown thrift server");
		}

		LOG.info("Successfully shutdown nimbus");
		// make sure shutdown nimbus
		JStormUtils.halt_process(0, "!!!Shutdown!!!");

	}

	private void initGroup(Map conf) throws Exception {

		if (data.isGroupMode() == false) {
			LOG.info("Group model has been disabled!");
			return;
		}

		LOG.info("Nimbus is on group model!");
		initGroupFileWatcher(ConfigExtension.getGroupFilePath(conf), data);
		flushGroupFile(ConfigExtension.getGroupFilePath(conf), data);

		data.getFlushGroupFileLock().lock();

		try {
			NimbusUtils.synchronizeGroupToTopology(data);

			NimbusUtils.synchronizeGroupToResource(data);
		} finally {

			data.getFlushGroupFileLock().unlock();
		}
	}

	private class FileListenerAdaptor extends FileAlterationListenerAdaptor {

		private String fileName;

		private String filePath;

		private NimbusData data;

		public FileListenerAdaptor(String fileName, String filePath,
				NimbusData data) {
			super();
			this.fileName = fileName;
			this.filePath = filePath;
			this.data = data;
		}

		@Override
		public void onFileCreate(File file) {
			// TODO Auto-generated method stub
			super.onFileCreate(file);
			if (file.getName().equals(fileName)) {
				try {
					flushGroupFile(filePath, data);
				} catch (Exception e) {
					LOG.error("", e);
				}
			}
		}

		@Override
		public void onFileChange(File file) {
			// TODO Auto-generated method stub
			super.onFileChange(file);
			if (file.getName().equals(fileName)) {
				try {
					flushGroupFile(filePath, data);
				} catch (Exception e) {
					LOG.error("", e);
				}
			}
		}

		@Override
		public void onFileDelete(File file) {
			// TODO Auto-generated method stub
			super.onFileDelete(file);
			if (file.getName().equals(fileName)) {
				try {
					flushGroupFile(filePath, data);
				} catch (Exception e) {
					LOG.error("", e);
				}
			}
		}
	}

}
