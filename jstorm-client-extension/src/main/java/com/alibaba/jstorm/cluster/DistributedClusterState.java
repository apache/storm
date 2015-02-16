package com.alibaba.jstorm.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.ClusterStateCallback;
import com.alibaba.jstorm.callback.WatcherCallBack;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.zk.Zookeeper;

/**
 * All ZK interface implementation
 * 
 * @author yannian.mu
 * 
 */
public class DistributedClusterState implements ClusterState {

	private static Logger LOG = Logger.getLogger(DistributedClusterState.class);

	private Zookeeper zkobj = new Zookeeper();
	private CuratorFramework zk;
	private WatcherCallBack watcher;

	/**
	 * why run all callbacks, when receive one event
	 */
	private ConcurrentHashMap<UUID, ClusterStateCallback> callbacks = new ConcurrentHashMap<UUID, ClusterStateCallback>();

	private Map<Object, Object> conf;
	private AtomicBoolean active;

	public DistributedClusterState(Map<Object, Object> _conf) throws Exception {
		conf = _conf;

		// just mkdir STORM_ZOOKEEPER_ROOT dir
		CuratorFramework _zk = mkZk();
		String path = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT));
		zkobj.mkdirs(_zk, path);
		_zk.close();

		active = new AtomicBoolean(true);

		watcher = new WatcherCallBack() {
			@Override
			public void execute(KeeperState state, EventType type, String path) {
				if (active.get()) {
					if (!(state.equals(KeeperState.SyncConnected))) {
						LOG.warn("Received event " + state + ":" + type + ":"
								+ path + " with disconnected Zookeeper.");
					} else {
						LOG.info("Received event " + state + ":" + type + ":"
								+ path);
					}

					if (!type.equals(EventType.None)) {
						for (Entry<UUID, ClusterStateCallback> e : callbacks
								.entrySet()) {
							ClusterStateCallback fn = e.getValue();
							fn.execute(type, path);
						}
					}
				}
			}
		};
		zk = null;
		zk = mkZk(watcher);

	}

	@SuppressWarnings("unchecked")
	private CuratorFramework mkZk() throws IOException {
		return zkobj.mkClient(conf,
				(List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
				conf.get(Config.STORM_ZOOKEEPER_PORT), "");
	}

	@SuppressWarnings("unchecked")
	private CuratorFramework mkZk(WatcherCallBack watcher)
			throws NumberFormatException, IOException {
		return zkobj.mkClient(conf,
				(List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
				conf.get(Config.STORM_ZOOKEEPER_PORT),
				String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT)), watcher);
	}

	@Override
	public void close() {
		this.active.set(false);
		zk.close();
	}

	@Override
	public void delete_node(String path) throws Exception {
		zkobj.deletereRcursive(zk, path);
	}

	@Override
	public List<String> get_children(String path, boolean watch)
			throws Exception {
		return zkobj.getChildren(zk, path, watch);
	}

	@Override
	public byte[] get_data(String path, boolean watch) throws Exception {
		return zkobj.getData(zk, path, watch);
	}

	@Override
	public void mkdirs(String path) throws Exception {
		zkobj.mkdirs(zk, path);

	}

	@Override
	public void set_data(String path, byte[] data) throws Exception {
	    if (data.length > (JStormUtils.SIZE_1_K * 800))
	        throw new Exception("Writing 800k+ data into ZK is not allowed!");
		if (zkobj.exists(zk, path, false)) {
			zkobj.setData(zk, path, data);
		} else {
			zkobj.mkdirs(zk, PathUtils.parent_path(path));
			zkobj.createNode(zk, path, data, CreateMode.PERSISTENT);
		}

	}

	@Override
	public void set_ephemeral_node(String path, byte[] data) throws Exception {
		zkobj.mkdirs(zk, PathUtils.parent_path(path));
		if (zkobj.exists(zk, path, false)) {
			zkobj.setData(zk, path, data);
		} else {
			zkobj.createNode(zk, path, data, CreateMode.EPHEMERAL);
		}
	}

	@Override
	public UUID register(ClusterStateCallback callback) {
		UUID id = UUID.randomUUID();
		this.callbacks.put(id, callback);
		return id;
	}

	@Override
	public ClusterStateCallback unregister(UUID id) {
		return this.callbacks.remove(id);
	}

	@Override
	public boolean node_existed(String path, boolean watch) throws Exception {
		// TODO Auto-generated method stub
		return zkobj.existsNode(zk, path, watch);
	}

	@Override
	public void tryToBeLeader(String path, byte[] host) throws Exception {
		// TODO Auto-generated method stub
		zkobj.createNode(zk, path, host, CreateMode.EPHEMERAL);
	}
}
