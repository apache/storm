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
package org.apache.storm.trident.topology.state;


import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.json.simple.JSONValue;

public class TransactionalState {

    public static final String USER = "user";
    public static final String COORDINATOR = "coordinator";
    private ITransactionalStateStorage transactionalStateStorage;

    public static TransactionalState newUserState(Map conf, String id) {
        return new TransactionalState(conf, id, USER);
    }

    public static TransactionalState newCoordinatorState(Map conf, String id) {
        return new TransactionalState(conf, id, COORDINATOR);
    }

    protected TransactionalState(Map conf, String id, String subroot) {
        try {
            String className = null;
            if (conf.get(Config.STORM_TRANSATION_STATE_STORE_FACTORY) != null) {
                className = (String) conf.get(Config.STORM_TRANSATION_STATE_STORE_FACTORY);
            } else {
                className = "org.apache.storm.trident.topology.state.TransactionalStateStorageZkFactory";
            }
            Class clazz = Class.forName(className);
            ITransactionalStateStorageFactory storageFactory = (ITransactionalStateStorageFactory) clazz.newInstance();
            transactionalStateStorage = storageFactory.mkTransactionalState(conf, id, subroot);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setData(String path, Object obj) {
        transactionalStateStorage.setData(path, obj);
    }

    public void delete(String path) {
        transactionalStateStorage.delete(path);
    }

    public List<String> list(String path) {
        return transactionalStateStorage.list(path);
    }

    public void mkdir(String path) {
        transactionalStateStorage.mkdir(path);
    }

    public Object getData(String path) {
        return transactionalStateStorage.getData(path);
    }

    public void close() {
        transactionalStateStorage.close();
    }
}
