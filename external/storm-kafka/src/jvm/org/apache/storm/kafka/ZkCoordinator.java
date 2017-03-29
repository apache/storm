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
package org.apache.storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;

import java.util.*;

import static org.apache.storm.kafka.KafkaUtils.taskPrefix;

public class ZkCoordinator implements PartitionCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    int _taskId;
    String _topologyInstanceId;
    Map<Partition, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList = new ArrayList<PartitionManager>();
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    ZkState _state;
    Map _topoConf;

    public ZkCoordinator(DynamicPartitionConnections connections, Map<String, Object> topoConf, SpoutConfig spoutConfig, ZkState state,
            int taskIndex, int totalTasks, int taskId, String topologyInstanceId) {
        this(connections, topoConf, spoutConfig, state, taskIndex, totalTasks, taskId, topologyInstanceId, buildReader(topoConf, spoutConfig));
    }

    public ZkCoordinator(DynamicPartitionConnections connections, Map<String, Object> topoConf, SpoutConfig spoutConfig, ZkState state,
            int taskIndex, int totalTasks, int taskId, String topologyInstanceId, DynamicBrokersReader reader) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _taskId = taskId;
        _topologyInstanceId = topologyInstanceId;
        _topoConf = topoConf;
        _state = state;
        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = reader;
    }

    private static DynamicBrokersReader buildReader(Map<String, Object> topoConf, SpoutConfig spoutConfig) {
        ZkHosts hosts = (ZkHosts) spoutConfig.hosts;
        return new DynamicBrokersReader(topoConf, hosts.brokerZkStr, hosts.brokerZkPath, spoutConfig.topic);
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    @Override
    public void refresh() {
        try {
            LOG.info(taskPrefix(_taskIndex, _totalTasks, _taskId) + " Refreshing partition manager connections");
            List<GlobalPartitionInformation> brokerInfo = _reader.getBrokerInfo();
            List<Partition> mine = KafkaUtils.calculatePartitionsForTask(brokerInfo, _totalTasks, _taskIndex, _taskId);

            Set<Partition> curr = _managers.keySet();
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            newPartitions.removeAll(curr);

            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            deletedPartitions.removeAll(mine);

            LOG.info(taskPrefix(_taskIndex, _totalTasks, _taskId) + " Deleted partition managers: " + deletedPartitions.toString());

            Map<Integer, PartitionManager> deletedManagers = new HashMap<>();
            for (Partition id : deletedPartitions) {
                deletedManagers.put(id.partition, _managers.remove(id));
            }
            for (PartitionManager manager : deletedManagers.values()) {
                if (manager != null) manager.close();
            }
            LOG.info(taskPrefix(_taskIndex, _totalTasks, _taskId) + " New partition managers: " + newPartitions.toString());

            for (Partition id : newPartitions) {
                PartitionManager man = new PartitionManager(
                        _connections,
                        _topologyInstanceId,
                        _state,
                        _topoConf,
                        _spoutConfig,
                        id,
                        deletedManagers.get(id.partition));
                _managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info(taskPrefix(_taskIndex, _totalTasks, _taskId) + " Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
}
