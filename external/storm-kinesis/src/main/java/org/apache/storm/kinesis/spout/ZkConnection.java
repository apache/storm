/**
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
 */

package org.apache.storm.kinesis.spout;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;

class ZkConnection {

    private final ZkInfo zkInfo;
    private CuratorFramework curatorFramework;

    ZkConnection(ZkInfo zkInfo) {
        this.zkInfo = zkInfo;
    }

    void initialize() {
        curatorFramework = CuratorFrameworkFactory.newClient(zkInfo.getZkUrl(),
                zkInfo.getSessionTimeoutMs(),
                zkInfo.getConnectionTimeoutMs(),
                new RetryNTimes(zkInfo.getRetryAttempts(), zkInfo.getRetryIntervalMs()));
        curatorFramework.start();
    }

    void commitState(String stream, String shardId, Map<Object, Object> state) {
        byte[] bytes = JSONValue.toJSONString(state).getBytes(Charset.forName("UTF-8"));
        try {
            String path = getZkPath(stream, shardId);
            if (curatorFramework.checkExists().forPath(path) == null) {
                curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, bytes);
            } else {
                curatorFramework.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Map<Object, Object> readState(String stream, String shardId) {
        try {
            String path = getZkPath(stream, shardId);
            Map<Object, Object> state = null;
            byte[] b = null;
            if (curatorFramework.checkExists().forPath(path) != null) {
                b = curatorFramework.getData().forPath(path);
            }
            if (b != null) {
                state = (Map<Object, Object>) JSONValue.parseWithException(new String(b, "UTF-8"));
            }
            return state;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void shutdown() {
        curatorFramework.close();
    }

    private String getZkPath(String stream, String shardId) {
        String path = "";
        if (!zkInfo.getZkNode().startsWith("/")) {
            path += "/";
        }
        path += zkInfo.getZkNode();
        if (!zkInfo.getZkNode().endsWith("/")) {
            path += "/";
        }
        path += (stream + "/" + shardId);
        return path;
    }
}
