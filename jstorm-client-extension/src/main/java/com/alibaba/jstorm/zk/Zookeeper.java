package com.alibaba.jstorm.zk;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.DefaultWatcherCallBack;
import com.alibaba.jstorm.callback.WatcherCallBack;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * ZK simple wrapper
 * 
 * @author yannian
 * 
 */
public class Zookeeper {

	private static Logger LOG = Logger.getLogger(Zookeeper.class);

	public CuratorFramework mkClient(Map conf, List<String> servers,
			Object port, String root) {
		return mkClient(conf, servers, port, root, new DefaultWatcherCallBack());
	}

	/**
	 * connect ZK, register Watch/unhandle Watch
	 * 
	 * @return
	 */
	public CuratorFramework mkClient(Map conf, List<String> servers,
			Object port, String root, final WatcherCallBack watcher) {

		CuratorFramework fk = Utils.newCurator(conf, servers, port, root);

		fk.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework _fk, CuratorEvent e)
					throws Exception {
				if (e.getType().equals(CuratorEventType.WATCHED)) {
					WatchedEvent event = e.getWatchedEvent();

					watcher.execute(event.getState(), event.getType(),
							event.getPath());
				}

			}
		});

		fk.getUnhandledErrorListenable().addListener(
				new UnhandledErrorListener() {
					@Override
					public void unhandledError(String msg, Throwable error) {
						String errmsg = "Unrecoverable Zookeeper error, halting process: "
								+ msg;
						LOG.error(errmsg, error);
						JStormUtils.halt_process(1,
								"Unrecoverable Zookeeper error");

					}
				});
		fk.start();
		return fk;
	}

	public String createNode(CuratorFramework zk, String path, byte[] data,
			org.apache.zookeeper.CreateMode mode) throws Exception {

		String npath = PathUtils.normalize_path(path);

		return zk.create().withMode(mode).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
				.forPath(npath, data);
	}

	public String createNode(CuratorFramework zk, String path, byte[] data)
			throws Exception {
		return createNode(zk, path, data,
				org.apache.zookeeper.CreateMode.PERSISTENT);
	}

	public boolean existsNode(CuratorFramework zk, String path, boolean watch)
			throws Exception {
		Stat stat = null;
		if (watch) {
			stat = zk.checkExists().watched()
					.forPath(PathUtils.normalize_path(path));
		} else {
			stat = zk.checkExists().forPath(PathUtils.normalize_path(path));
		}
		return stat != null;
	}

	public void deleteNode(CuratorFramework zk, String path) throws Exception {
		zk.delete().forPath(PathUtils.normalize_path(path));
	}

	public void mkdirs(CuratorFramework zk, String path) throws Exception {

		String npath = PathUtils.normalize_path(path);

		// the node is "/"
		if (npath.equals("/")) {
			return;
		}

		// the node exist
		if (existsNode(zk, npath, false)) {
			return;
		}

		mkdirs(zk, PathUtils.parent_path(npath));
		try {
			createNode(zk, npath, JStormUtils.barr((byte) 7),
					org.apache.zookeeper.CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			;// this can happen when multiple clients doing mkdir at same
				// time
			LOG.warn("zookeeper mkdirs for path" + path, e);

		}

	}

	public byte[] getData(CuratorFramework zk, String path, boolean watch)
			throws Exception {
		String npath = PathUtils.normalize_path(path);
		try {
			if (existsNode(zk, npath, watch)) {
				if (watch) {
					return zk.getData().watched().forPath(npath);
				} else {
					return zk.getData().forPath(npath);
				}
			}
		} catch (KeeperException e) {
			LOG.error("zookeeper getdata for path" + path, e);
		}

		return null;
	}

	public List<String> getChildren(CuratorFramework zk, String path,
			boolean watch) throws Exception {

		String npath = PathUtils.normalize_path(path);

		if (watch) {
			return zk.getChildren().watched().forPath(npath);
		} else {
			return zk.getChildren().forPath(npath);
		}
	}

	public Stat setData(CuratorFramework zk, String path, byte[] data)
			throws Exception {
		String npath = PathUtils.normalize_path(path);
		return zk.setData().forPath(npath, data);
	}

	public boolean exists(CuratorFramework zk, String path, boolean watch)
			throws Exception {
		return existsNode(zk, path, watch);
	}

	public void deletereRcursive(CuratorFramework zk, String path)
			throws Exception {

		String npath = PathUtils.normalize_path(path);

		if (existsNode(zk, npath, false)) {

			List<String> childs = getChildren(zk, npath, false);

			for (String child : childs) {

				String childFullPath = PathUtils.full_path(npath, child);

				deletereRcursive(zk, childFullPath);
			}

			deleteNode(zk, npath);
		}
	}

	public static Factory mkInprocessZookeeper(String localdir, int port)
			throws IOException, InterruptedException {
		LOG.info("Starting inprocess zookeeper at port " + port + " and dir "
				+ localdir);
		File localfile = new File(localdir);
		ZooKeeperServer zk = new ZooKeeperServer(localfile, localfile, 2000);
		Factory factory = new Factory(new InetSocketAddress(port), 0);
		factory.startup(zk);
		return factory;
	}

	public void shutdownInprocessZookeeper(Factory handle) {
		handle.shutdown();
	}

}
