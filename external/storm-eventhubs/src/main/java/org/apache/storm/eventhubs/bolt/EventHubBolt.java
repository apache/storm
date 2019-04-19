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
package org.apache.storm.eventhubs.bolt;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.EventHubException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A bolt that writes event message to EventHub.
 */
public class EventHubBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(EventHubBolt.class);

	protected OutputCollector collector;
	protected EventHubClient ehClient;
	protected PartitionSender sender;
	protected EventHubBoltConfig boltConfig;
	protected ExecutorService threadpool;

	public EventHubBolt(String connectionString, String entityPath) {
		boltConfig = new EventHubBoltConfig(connectionString, entityPath);
	}

	public EventHubBolt(String userName, String password, String namespace,
			String entityPath, boolean partitionMode) {
		boltConfig = new EventHubBoltConfig(userName, password, namespace,
				entityPath, partitionMode);
	}

	public EventHubBolt(EventHubBoltConfig config) {
		boltConfig = config;
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.threadpool = Executors.newFixedThreadPool(4);
		logger.info(String.format("Conn String: %s, PartitionMode %s", this.boltConfig.getConnectionString(),
				String.valueOf(this.boltConfig.getPartitionMode())));
		try {
			this.ehClient = EventHubClient.createSync(this.boltConfig.getConnectionString(), Executors.newScheduledThreadPool(4));
			if (boltConfig.getPartitionMode()) {
				// We can use the task index (starting from 0) as the partition ID
				String myPartitionId = String.valueOf(context.getThisTaskIndex());
			    logger.info("Writing to partition id: " + myPartitionId);
				this.sender = ehClient.createPartitionSenderSync(myPartitionId);
			}
		} catch (Exception ex) {
			this.collector.reportError(ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void execute(Tuple tuple) {
		EventData sendEvent = EventData.create(boltConfig.getEventDataFormat().serialize(tuple));
		CompletableFuture<Void> future = boltConfig.getPartitionMode() ? this.sender.send(sendEvent) : this.ehClient.send(sendEvent);
		future.whenCompleteAsync((unused, e) -> {
			if (e == null) {
				this.collector.ack(tuple);
			} else {
				this.collector.reportError(e);
				this.collector.fail(tuple);
			}
		}, this.threadpool);
	}

	@Override
	public void cleanup() {
		logger.debug("EventHubBolt cleanup");
		if (this.sender != null) {
			try {
				this.sender.closeSync();
			} catch (EventHubException e) {
				logger.error("Exception during EventHubBolt cleanup phase" + e.toString());
			}
			this.sender = null;
		}
		if (this.ehClient != null) {
			try {
				this.ehClient.closeSync();
			} catch (EventHubException e) {
				logger.error("Exception during EventHubBolt cleanup phase" + e.toString());
			}
			this.ehClient =  null;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
