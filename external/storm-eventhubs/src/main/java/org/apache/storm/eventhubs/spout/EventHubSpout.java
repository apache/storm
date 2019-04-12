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
package org.apache.storm.eventhubs.spout;

import com.google.common.base.Strings;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.eventhubs.core.EventHubReceiverImpl;
import org.apache.storm.eventhubs.core.EventHubConfig;
import org.apache.storm.eventhubs.core.EventHubMessage;
import org.apache.storm.eventhubs.core.IEventHubReceiver;
import org.apache.storm.eventhubs.core.IEventHubReceiverFactory;
import org.apache.storm.eventhubs.core.IPartitionCoordinator;
import org.apache.storm.eventhubs.core.IPartitionManager;
import org.apache.storm.eventhubs.core.IPartitionManagerFactory;
import org.apache.storm.eventhubs.core.MessageId;
import org.apache.storm.eventhubs.core.PartitionManager;
import org.apache.storm.eventhubs.core.StaticPartitionCoordinator;
import org.apache.storm.eventhubs.state.IStateStore;
import org.apache.storm.eventhubs.state.ZookeeperStateStore;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EventHubSpout extends BaseRichSpout {
	private static final long serialVersionUID = -8460916098313963614L;
    private static final Logger logger = LoggerFactory.getLogger(EventHubSpout.class);

    private final EventHubSpoutConfig eventHubSpoutConfig;

    private final int checkpointIntervalInSeconds;
    private IStateStore stateStore;
    private IPartitionCoordinator partitionCoordinator;
    private IPartitionManagerFactory pmFactory;
    private IEventHubReceiverFactory recvFactory;
    private SpoutOutputCollector collector;
    private long lastCheckpointTime;
    private int currentPartitionIndex = -1;
  
    public EventHubSpout(String username, String password, String namespace,
            String entityPath, int partitionCount) {
        this(new EventHubSpoutConfig(username, password, namespace, entityPath, partitionCount));
    }

    public EventHubSpout(String username, String password, String namespace,
	        String entityPath, int partitionCount, int batchSize) {
	    this(new EventHubSpoutConfig(username, password, namespace, entityPath, partitionCount, batchSize));
	}
  
    public EventHubSpout(EventHubSpoutConfig spoutConfig) {
        this(spoutConfig, null, null, null);
    }
  
    public EventHubSpout(EventHubSpoutConfig spoutConfig, IStateStore store, IPartitionManagerFactory pmFactory,
            IEventHubReceiverFactory recvFactory) {
        this.eventHubSpoutConfig = spoutConfig;
        this.checkpointIntervalInSeconds = spoutConfig.getCheckpointIntervalInSeconds();
        this.lastCheckpointTime = System.currentTimeMillis();
        this.stateStore = store;
    
        this.pmFactory = pmFactory;
        if (this.pmFactory == null) {
            this.pmFactory = new IPartitionManagerFactory() {
    	        private static final long serialVersionUID = -3134660797825594845L;
    	  
                @Override
                public IPartitionManager create(EventHubConfig spoutConfig, String partitionId,
                		IStateStore stateStore, IEventHubReceiver receiver) {
                    return new PartitionManager(spoutConfig, partitionId,stateStore, receiver);
                }
            };
        }
    
        this.recvFactory = recvFactory;
        if (this.recvFactory == null) {
            this.recvFactory = new IEventHubReceiverFactory() {
    	        private static final long serialVersionUID = -7215384402396274196L;
    	  
                @Override
                public IEventHubReceiver create(EventHubConfig spoutConfig, String partitionId) {
                    return new EventHubReceiverImpl(spoutConfig, partitionId);
                }
            };
        }
    }
  
    /**
     * This is a extracted method that is easy to test
     * @param config
     * @param totalTasks
     * @param taskIndex
     * @param collector
     * @throws Exception
     */
    // TODO private?
    public void preparePartitions(Map<String, Object> config, int totalTasks, int taskIndex,
    		SpoutOutputCollector collector) throws Exception {
        this.collector = collector;
        if (this.stateStore == null) {
            String zkEndpointAddress = eventHubSpoutConfig.getZkConnectionString();
            if (StringUtils.isBlank(zkEndpointAddress)) {
                // use storm's zookeeper servers if not specified.
                @SuppressWarnings("unchecked")
				List<String> zkServers = (List<String>)config.get(Config.STORM_ZOOKEEPER_SERVERS);
                Integer zkPort = ((Number)config.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
                StringBuilder sb = new StringBuilder();
                for (String zk : zkServers) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(zk + ":" + zkPort);
                }
                zkEndpointAddress = sb.toString();
            }
            this.stateStore = new ZookeeperStateStore(zkEndpointAddress,
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_TIMES).toString()),
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL).toString()));
        }
        this.stateStore.open();

        logger.info("TaskIndex: " + taskIndex + ", TotalTasks: " + totalTasks + ", Total Partitions: " +
        this.eventHubSpoutConfig.getPartitionCount());
    
        this.partitionCoordinator = new StaticPartitionCoordinator(this.eventHubSpoutConfig, taskIndex,
        		totalTasks, this.stateStore, this.pmFactory, this.recvFactory);

        for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
            partitionManager.open();
        }
    }

    @Override
    public void open(Map<String, Object> config, TopologyContext context, SpoutOutputCollector collector) {
        logger.debug("EventHubSpout start: open()");
        String topologyName = (String)config.get(Config.TOPOLOGY_NAME);
        this.eventHubSpoutConfig.setTopologyName(topologyName);

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int taskIndex = context.getThisTaskIndex();
        if (totalTasks > eventHubSpoutConfig.getPartitionCount()) {
            throw new RuntimeException("Total tasks of EventHubSpout " + totalTasks +
            		" is greater than partition count: " + this.eventHubSpoutConfig.getPartitionCount());
        }

        logger.info(String.format("TopologyName: %s, TotalTasks: %d, TaskIndex: %d", topologyName, totalTasks,
        		taskIndex));

        try {
            preparePartitions(config, totalTasks, taskIndex, collector);
        } catch (Exception e) {
	        collector.reportError(e);
            throw new RuntimeException(e);
        }
    
        // register metrics
        context.registerMetric("EventHubReceiver", new IMetric() {
	            @Override
	            public Object getValueAndReset() {
	                Map<String, Object> concatMetricsDataMaps = new HashMap<String, Object>();
	                for (IPartitionManager partitionManager :
	        	            EventHubSpout.this.partitionCoordinator.getMyPartitionManagers()) {
	                    concatMetricsDataMaps.putAll(partitionManager.getMetricsData());
	                }
	                return concatMetricsDataMaps;
	            }
	        }, Integer.parseInt(config.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS).toString()));
        logger.info("EventHubSpout end: open()");
    }

    @Override
    public void nextTuple() {
    	List<IPartitionManager> partitionManagers = partitionCoordinator.getMyPartitionManagers();
    	EventHubMessage eventHubMessage = null;

    	for (int i = 0; i < partitionManagers.size(); i++) {
    		this.currentPartitionIndex = (this.currentPartitionIndex + 1) % partitionManagers.size();
    		IPartitionManager partitionManager = partitionManagers.get(this.currentPartitionIndex);

    		if (partitionManager == null) {
    			throw new RuntimeException("A PartitionManager for partitionid: " + this.currentPartitionIndex +
    					" doesn't exist.");
    		}

    		eventHubMessage = partitionManager.receive();
    		if (eventHubMessage != null) {
    			break;
    		}
    	}

    	if (eventHubMessage != null) {
    		MessageId messageId = eventHubMessage.getMessageId();
    		List<Object> tuples = this.eventHubSpoutConfig.getEventDataScheme().deserialize(eventHubMessage);
    		if (tuples != null) { // TODO not in JAR?
    			collector.emit(tuples, messageId);
    		}
    	}
    
    	checkpointIfNeeded();

    	// We don't need to sleep here because the IPartitionManager.receive() is
    	// a blocked call so it's fine to call this function in a tight loop.
    }

    @Override
    public void ack(Object msgId) {
    	MessageId messageId = (MessageId)msgId;
	    IPartitionManager partitionManager = this.partitionCoordinator.getPartitionManager(messageId.getPartitionId());
	    String offset = messageId.getOffset();
	    partitionManager.ack(offset);
    }

	@Override
	public void fail(Object msgId) {
	    MessageId messageId = (MessageId)msgId;
	    IPartitionManager partitionManager = this.partitionCoordinator.getPartitionManager(messageId.getPartitionId());
	    String offset = messageId.getOffset();
	    partitionManager.fail(offset);
	}

	@Override
	public void deactivate() {
		// let's checkpoint so that we can get the last checkpoint when restarting.
		checkpoint();
	}
	
	@Override
	public void close() {
	    for (IPartitionManager partitionManager : this.partitionCoordinator.getMyPartitionManagers()) {
	        partitionManager.close();
	    }
	    this.stateStore.close();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fields = new LinkedList<String>();
		fields.add("message");
		// TODO this.eventHubSpoutConfig.getEventDataScheme().getOutputFields() instead ???
		  
	    if (Strings.isNullOrEmpty(this.eventHubSpoutConfig.getOutputStreamId())) {
	    	declarer.declare(new Fields(fields));
	    } else {
	    	declarer.declareStream(this.eventHubSpoutConfig.getOutputStreamId(), new Fields(fields));
	    }
  	}

	private void checkpointIfNeeded() {
		long nextCheckpointTime = this.lastCheckpointTime + (this.checkpointIntervalInSeconds * 1000);
		if (nextCheckpointTime < System.currentTimeMillis()) {
			checkpoint();
			this.lastCheckpointTime = System.currentTimeMillis();
		}
	}
  
	private void checkpoint() {
		for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
			partitionManager.checkpoint();
		}
	}
}
