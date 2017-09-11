/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.apache.storm.eventhubs.state;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.servicebus.StringUtil;

/**
 * Zookeeper based implementation of the state store.
 * 
 * @see IStateStore
 */
public class ZookeeperStateStore implements IStateStore {
	private static final long serialVersionUID = -995647135239199102L;
	private static final Logger logger = LoggerFactory.getLogger(ZookeeperStateStore.class);
	private static final int DEFAULT_RETRIES = 3;
	private static final int DEFAULT_RETRY_INTERVAL_MS = 100;
	private static final String ZK_LOCAL_URL = "localhost:2181";

	private final String zookeeperConnectionString;
	private final CuratorFramework curatorFramework;

	/**
	 * Creates a new instance. No connection to Zookeeper is established yet.
	 * 
	 * @param zookeeperConnectionString
	 *            Zookeeper connection string
	 */
	public ZookeeperStateStore(String zookeeperConnectionString) {
		this(zookeeperConnectionString, DEFAULT_RETRIES, DEFAULT_RETRY_INTERVAL_MS);
	}

	/**
	 * Creates a new instance. No connection to Zookeeper is established yet.
	 * 
	 * @param connectionString
	 *            Zookeeper connection string (example: zk1.azurehdinsight.net:2181)
	 * @param retries
	 *            number of times to retry for transient failures
	 * @param retryInterval
	 *            Sleep interval (in ms) between retry attempts
	 */
	public ZookeeperStateStore(String connectionString, int retries, int retryInterval) {
		zookeeperConnectionString = StringUtil.isNullOrWhiteSpace(connectionString) ? ZK_LOCAL_URL : connectionString;
		logger.debug("using ZKConnectionString: " + zookeeperConnectionString);

		curatorFramework = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
				new RetryNTimes(retries, retryInterval));
	}

	@Override
	public void open() {
		curatorFramework.start();
	}

	@Override
	public void close() {
		curatorFramework.close();
	}

	@Override
	public void saveData(String statePath, String data) {
		data = StringUtil.isNullOrWhiteSpace(data) ? "" : data;
		byte[] bytes = data.getBytes();

		try {
			if (curatorFramework.checkExists().forPath(statePath) == null) {
				curatorFramework.create().creatingParentsIfNeeded().forPath(statePath, bytes);
			} else {
				curatorFramework.setData().forPath(statePath, bytes);
			}

			logger.debug(String.format("data was saved. path: %s, data: %s.", statePath, data));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String readData(String statePath) {
		try {
			if (curatorFramework.checkExists().forPath(statePath) == null) {
				return null;
			}

			String data = new String(curatorFramework.getData().forPath(statePath));
			logger.debug(String.format("data was retrieved. path: %s, data: %s.", statePath, data));

			return data;

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
